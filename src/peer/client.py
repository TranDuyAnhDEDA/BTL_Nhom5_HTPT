import os
import time
import socket
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Tuple, Set
import requests

from ..core.metadata import FileMetadata
from ..core.chunker import verify_chunk, merge_chunks
from .protocol import send_packet, recv_packet

# Cấu hình logging
logger = logging.getLogger("Peer")


def get_local_ip() -> str:
    """Tự động phát hiện địa chỉ IP LAN của máy (ví dụ 192.168.1.x) để các máy khác kết nối được."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


class PeerNode:
    """
    Đại diện cho một Nút mạng P2P (Peer):
    - Đóng vai SERVER: Lắng nghe kết nối TCP để Upload các chunk cho peer khác.
    - Đóng vai CLIENT: Kết nối tới các peer khác để Download các chunk còn thiếu (hỗ trợ Tải song song).
    - Giao tiếp với Tracker: Đăng ký file, Announce bitfield và gửi Heartbeat.
    """
    def __init__(
        self,
        peer_id: str,
        host: Optional[str] = None,
        port: int = 6001,
        storage_dir: str = "./storage",
        tracker_url: str = "http://127.0.0.1:8000",
        listen_host: str = "0.0.0.0"
    ):
        self.peer_id = peer_id

        # Nếu host không truyền, ưu tiên biến môi trường PEER_HOST hoặc tự phát hiện IP LAN
        env_host = os.environ.get("PEER_HOST")
        if host is not None:
            self.host = host
        elif env_host:
            self.host = env_host
        else:
            self.host = get_local_ip()

        self.port = port
        self.listen_host = listen_host
        self.storage_dir = storage_dir
        self.tracker_url = tracker_url

        # Mảng bitfield lưu các chunk mình đang sở hữu theo từng file: {file_id: set(chunk_indices)}
        self.owned_chunks: Dict[str, Set[int]] = {}
        self._chunks_lock = threading.Lock()  # Khóa bảo vệ dữ liệu khi nhiều luồng cùng ghi

        # Trạng thái máy chủ socket lắng nghe upload
        self._server_sock: Optional[socket.socket] = None
        self._is_running = False
        self._server_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None

        # Đảm bảo thư mục lưu trữ tồn tại
        os.makedirs(self.storage_dir, exist_ok=True)

    def _log(self, level: str, msg: str):
        try:
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{level}] [{self.peer_id}@{self.port}]: {msg}")
        except Exception:
            try:
                safe_msg = msg.encode("ascii", errors="replace").decode("ascii")
                print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{level}] [{self.peer_id}@{self.port}]: {safe_msg}")
            except Exception:
                pass

    # =========================================================================
    # 1. PHẦN SERVER (UPLOAD WORKER - PHỤC VỤ CÁC MÁY KHÁC ĐẾN XIN DỮ LIỆU)
    # =========================================================================

    def start_upload_server(self):
        """Khởi động máy chủ Socket TCP để sẵn sàng upload các chunk khi có yêu cầu."""
        if self._is_running and self._server_sock:
            return
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Lắng nghe trên tất cả các card mạng (0.0.0.0) để nhận kết nối từ cả localhost và LAN
        self._server_sock.bind((self.listen_host, self.port))
        self._server_sock.listen(20)
        self._is_running = True

        self._server_thread = threading.Thread(target=self._listen_loop, daemon=True)
        self._server_thread.start()
        self._log("INFO", f"Máy chủ Upload đã mở cổng lắng nghe tại {self.listen_host}:{self.port} (Quảng bá IP: {self.host})")

    def _listen_loop(self):
        while self._is_running:
            try:
                client_sock, addr = self._server_sock.accept()
                threading.Thread(target=self._handle_client, args=(client_sock, addr), daemon=True).start()
            except Exception:
                break

    def _handle_client(self, client_sock: socket.socket, addr: Tuple[str, int]):
        """Xử lý yêu cầu xin chunk từ một peer khác."""
        try:
            header, _ = recv_packet(client_sock)
            action = header.get("action")

            if action == "REQUEST_CHUNK":
                file_id = header.get("file_id")
                chunk_idx = header.get("chunk_idx")

                # Tìm file chunk trên ổ cứng của mình
                chunk_file = os.path.join(self.storage_dir, "chunks", file_id, f"chunk_{chunk_idx}.bin")
                
                if os.path.exists(chunk_file):
                    with open(chunk_file, "rb") as cf:
                        chunk_data = cf.read()

                    self._log("INFO", f"Đang UPLOAD Chunk {chunk_idx} của file '{file_id}' cho {addr} ({len(chunk_data):,} bytes)")
                    send_packet(client_sock, {"status": "SUCCESS", "chunk_idx": chunk_idx}, chunk_data)
                else:
                    self._log("WARNING", f"Peer {addr} xin Chunk {chunk_idx} nhưng tôi không có!")
                    send_packet(client_sock, {"status": "ERROR", "message": f"Chunk {chunk_idx} not found"})

        except Exception as e:
            self._log("ERROR", f"Lỗi khi truyền dữ liệu cho {addr}: {e}")
        finally:
            client_sock.close()

    def stop(self):
        """Dừng hoàn toàn máy chủ socket."""
        self._is_running = False
        if self._server_sock:
            try:
                self._server_sock.close()
            except Exception:
                pass
            self._server_sock = None
        self._log("INFO", "Máy chủ Upload đã đóng.")

    # =========================================================================
    # 2. PHẦN CLIENT (DOWNLOAD WORKER & TẢI SONG SONG)
    # =========================================================================

    def download_chunk(
        self,
        peer_ip: str,
        peer_port: int,
        file_id: str,
        chunk_idx: int,
        expected_hash: str,
        auto_announce: bool = True,
        timeout: float = 3.0
    ) -> bool:
        """
        Kết nối trực tiếp tới một Peer khác qua Socket TCP để tải về 1 chunk.
        Hỗ trợ timeout ngắn để phát hiện sớm sự cố sập nguồn của peer nguồn.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)

        try:
            self._log("INFO", f"Kết nối tới {peer_ip}:{peer_port} để tải Chunk {chunk_idx}...")
            sock.connect((peer_ip, peer_port))

            # 1. Gửi gói tin yêu cầu
            req_header = {
                "action": "REQUEST_CHUNK",
                "file_id": file_id,
                "chunk_idx": chunk_idx
            }
            send_packet(sock, req_header)

            # 2. Nhận phản hồi
            resp_header, chunk_data = recv_packet(sock)

            if resp_header.get("status") != "SUCCESS":
                self._log("WARNING", f"Peer {peer_ip}:{peer_port} từ chối cung cấp: {resp_header.get('message')}")
                return False

            # 3. Soi mã băm (Kiểm tra tính toàn vẹn)
            if not verify_chunk(chunk_data, expected_hash):
                self._log("ERROR", f"CẢNH BÁO BẢO MẬT: Chunk {chunk_idx} từ {peer_ip}:{peer_port} SAI HASH (BỊ HỎNG HOẶC GIẢ MẠO)!")
                return False

            # 4. Lưu chunk chuẩn vào ổ cứng của mình
            chunk_dir = os.path.join(self.storage_dir, "chunks", file_id)
            os.makedirs(chunk_dir, exist_ok=True)
            chunk_file_path = os.path.join(chunk_dir, f"chunk_{chunk_idx}.bin")
            with open(chunk_file_path, "wb") as f:
                f.write(chunk_data)

            # Đánh dấu mình đã sở hữu chunk này (Dùng Lock để an toàn đa luồng)
            with self._chunks_lock:
                if file_id not in self.owned_chunks:
                    self.owned_chunks[file_id] = set()
                self.owned_chunks[file_id].add(chunk_idx)

            self._log("INFO", f"Tải & Xác thực THÀNH CÔNG Chunk {chunk_idx} ({len(chunk_data):,} bytes) từ {peer_ip}:{peer_port}")

            # BÁO DANH NGAY LẬP TỨC: Tôi đã có chunk này
            if auto_announce:
                self.announce_to_tracker(file_id)

            return True

        except (socket.timeout, ConnectionRefusedError, ConnectionResetError, ConnectionError, OSError) as e:
            self._log("WARNING", f"SỰ CỐ KẾT NỐI: Không thể tải Chunk {chunk_idx} từ {peer_ip}:{peer_port} ({e})")
            return False
        except Exception as e:
            self._log("ERROR", f"Lỗi không xác định khi tải Chunk {chunk_idx} từ {peer_ip}:{peer_port}: {e}")
            return False
        finally:
            sock.close()

    # =========================================================================
    # 3. GIAO TIẾP VỚI TRACKER (ĐIỀU PHỐI DANH BẠ)
    # =========================================================================

    def register_file_to_tracker(self, metadata: FileMetadata) -> bool:
        """Đăng ký file mới với Tracker."""
        try:
            url = f"{self.tracker_url}/register_file"
            res = requests.post(url, json=metadata.to_dict())
            if res.status_code == 201:
                self._log("INFO", f"Đã đăng ký file '{metadata.file_name}' lên Tracker.")
                return True
        except Exception as e:
            self._log("ERROR", f"Lỗi đăng ký file lên Tracker: {e}")
        return False

    def announce_to_tracker(self, file_id: str) -> bool:
        """Gửi danh sách chunk mình đang có (bitfield) lên Tracker."""
        try:
            url = f"{self.tracker_url}/announce"
            with self._chunks_lock:
                current_bitfield = list(sorted(self.owned_chunks.get(file_id, set())))

            payload = {
                "peer_id": self.peer_id,
                "file_id": file_id,
                "ip": self.host,
                "port": self.port,
                "bitfield": current_bitfield
            }
            res = requests.post(url, json=payload)
            if res.status_code == 200:
                self._log("INFO", f"Đã cập nhật bitfield lên Tracker: {current_bitfield}")
                return True
        except Exception as e:
            self._log("ERROR", f"Lỗi announce lên Tracker: {e}")
        return False

    def get_peers_from_tracker(self, file_id: str) -> List[dict]:
        """Hỏi Tracker danh sách các Peer đang có các chunk của file."""
        try:
            url = f"{self.tracker_url}/peers/{file_id}"
            res = requests.get(url)
            if res.status_code == 200:
                data = res.json()
                return data.get("peers", [])
        except Exception as e:
            self._log("ERROR", f"Lỗi tra cứu peers từ Tracker: {e}")
        return []

    def start_heartbeat(self, interval: float = 5.0):
        """Bắt đầu luồng gửi nhịp tim (Heartbeat) định kỳ tới Tracker."""
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return

        def _heartbeat_loop():
            while self._is_running:
                try:
                    requests.post(
                        f"{self.tracker_url}/heartbeat",
                        json={"peer_id": self.peer_id},
                        timeout=3.0
                    )
                except Exception:
                    pass
                time.sleep(interval)

        self._heartbeat_thread = threading.Thread(target=_heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()
        self._log("INFO", f"Đã kích hoạt gửi Heartbeat định kỳ mỗi {interval}s tới Tracker.")

    def seed_file(self, file_path: str, chunk_size: int = 512 * 1024, custom_file_id: Optional[str] = None) -> Optional[FileMetadata]:
        """
        Chia nhỏ file, lưu các chunk ra đĩa, đăng ký và announce lên Tracker,
        sau đó mở cổng Socket Server và gửi nhịp tim định kỳ.
        """
        from ..core.chunker import split_file
        import hashlib

        if not os.path.exists(file_path):
            self._log("ERROR", f"Không tìm thấy file nguồn: {file_path}")
            return None

        file_name = os.path.basename(file_path)
        file_id = custom_file_id or ("f_" + hashlib.md5(file_name.encode()).hexdigest()[:6])
        chunks_dir = os.path.join(self.storage_dir, "chunks", file_id)

        self._log("INFO", f"Đang chia nhỏ file '{file_name}' thành chunks {chunk_size // 1024} KB...")
        metadata = split_file(
            file_path=file_path,
            output_chunks_dir=chunks_dir,
            tracker_url=self.tracker_url,
            chunk_size=chunk_size,
            file_id=file_id
        )

        with self._chunks_lock:
            self.owned_chunks[file_id] = set(range(metadata.num_chunks))

        # Đăng ký và Announce lên Tracker
        if self.register_file_to_tracker(metadata):
            self.announce_to_tracker(file_id)

        # Khởi động Socket Server và Heartbeat
        self.start_upload_server()
        self.start_heartbeat()

        self._log("INFO", f"SẴN SÀNG CHIA SẺ! File ID: '{file_id}' ({metadata.num_chunks} chunks x {metadata.chunk_size // 1024} KB)")
        return metadata

