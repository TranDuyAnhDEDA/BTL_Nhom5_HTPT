# KHUNG BÁO CÁO CHI TIẾT ĐỒ ÁN MÔN HỌC HỆ THỐNG PHÂN TÁN
**Đề tài DS02:** Xây dựng Hệ thống Chia sẻ File Ngang hàng P2P Mô phỏng BitTorrent  
*(Bám sát 100% đặc tả Mục 6, Mục 9 và đủ 19 mục bắt buộc tại Mục 10.2 của Course Project Specification – Độ dài khuyến nghị: 20–30 trang chưa tính Phụ lục)*

---

## 1. Trang bìa (Cover Page) *(~1 trang)*
- Tên trường / Khoa / Bộ môn.
- Tên môn học: **Hệ thống phân tán (Distributed Systems)**.
- Tên đề tài: **DS02 – Hệ thống chia sẻ file P2P mô phỏng BitTorrent (P2P File-Sharing System Simulating BitTorrent)**.
- Giảng viên hướng dẫn.
- Thông tin sinh viên / nhóm thực hiện (Họ tên, MSSV, Lớp).
- Thời gian thực hiện (Tháng 11/2026).

---

## 2. Thông tin nhóm và phân công nhiệm vụ (Group Information and Task Assignment) *(~1 trang)*
- **Bảng danh sách thành viên:** STT, Họ tên, MSSV, GitHub Username, Vai trò.
- **Bảng phân công công việc chi tiết:**
  - *Gợi ý các hạng mục:* Thiết kế kiến trúc & giao thức; Module Core (`chunker.py`, `metadata.py`); Tracker Server (`server.py`); P2P Socket Client & Failover (`protocol.py`, `client.py`); Công cụ LAN CLI (`run_peer.py`); Web Dashboard (`index.html`, `scenario_runner.py`); Docker & Scripts (`deploy/`, `scripts/`); Kiểm thử (`tests/`) & Báo cáo (`docs/`).
- **Minh chứng đóng góp trên GitHub:** Tóm tắt tỷ lệ commit và link tới repo `https://github.com/TranDuyAnhDEDA/BTL_HTPT`.

---

## 3. Tóm tắt đồ án (Abstract) *(~0.5–1 trang)*
- Tóm tắt ngắn gọn (200–300 từ):
  - **Bài toán:** Khắc phục điểm nghẽn cổ chai băng thông của mô hình Client-Server truyền thống khi nhiều người cùng tải một file dung lượng lớn.
  - **Giải pháp:** Thiết kế và cài đặt hệ thống chia sẻ file P2P mô phỏng BitTorrent gồm Central Tracker (FastAPI) chỉ quản lý Metadata và danh bạ Peer, kết hợp các Peer độc lập truyền dữ liệu nhị phân trực tiếp qua TCP Socket với cơ chế chia mảnh 512 KB, xác thực SHA-256, tải song song đa luồng, vừa tải vừa chia sẻ (Seed-while-Leeching) và tự động chịu lỗi (Failover).
  - **Kết quả:** Đạt 7/7 kịch bản demo bắt buộc, hoạt động được cả trên máy đơn (đa tiến trình/Docker) và nhiều máy tính trong mạng LAN, kèm giao diện Web Dashboard theo dõi trực quan.

---

## 4. Mô tả bài toán và yêu cầu (Problem Description and Requirements) *(~2 trang)*
### 4.1. Bối cảnh và bài toán thực tế
- Hạn chế của kiến trúc tập trung (Client-Server): quá tải server, Single Point of Failure về dữ liệu, tốn băng thông upload của máy chủ trung tâm.
- Nguyên lý BitTorrent: chia nhỏ file thành chunks, người tải cũng trở thành người chia sẻ (Leecher $\rightarrow$ Seeder).

### 4.2. Yêu cầu chức năng (Functional Requirements – theo Mục 6.3)
- **Metadata:** Chứa đủ `file_id`, `file_name`, `file_size`, `chunk_size`, `num_chunks`, `chunk_hashes` (SHA-256 từng mảnh), `file_hash` (SHA-256 toàn file), `tracker_url`.
- **Chia sẻ & Khám phá Peer:** Cắt file thành chunks, đăng ký lên Tracker, quản lý danh sách Peer sở hữu một phần hoặc toàn bộ file, cơ chế `announce` bitfield.
- **Tải file:** Tải song song từ $\ge 2$ peers, tải nhiều chunk đồng thời, vừa tải vừa upload được chunk đã có, giới hạn số luồng tải, tự yêu cầu lại chunk lỗi từ peer khác.
- **Toàn vẹn & Khôi phục:** Kiểm tra hash từng chunk, từ chối chunk hỏng, ghép đúng thứ tự, xác thực hash file cuối, tiếp tục tải khi peer nguồn rời mạng, hỗ trợ Resume (không tải lại chunk đã có).

### 4.3. Yêu cầu phi chức năng (Non-Functional Requirements – theo Mục 4)
- Tính phân tán thực sự ($\ge 3$ tiến trình/container/máy độc lập giao tiếp qua mạng, không dùng biến toàn cục hay file chung thay mạng).
- Logging rõ ràng (timestamp, node ID, loại sự kiện, kết quả).
- Khả năng chịu lỗi, tính nhất quán và an toàn cơ bản.

---

## 5. Các vấn đề của Hệ thống phân tán (Distributed-System Issues) *(~1.5–2 trang)*
Phân tích 6 bài toán cốt lõi hệ thống phải giải quyết:
1. **Giao tiếp liên tiến trình qua mạng (IPC & Framing):** Dữ liệu TCP dạng luồng byte (stream-oriented) dễ bị dính gói hoặc phân mảnh $\rightarrow$ cần thiết kế giao thức đóng gói Framing.
2. **Định danh và khám phá nút mạng (Naming, Discovery & Churn):** Peer vào/ra liên tục (churn), địa chỉ IP/Port động trên LAN $\rightarrow$ cần Tracker và cơ chế Heartbeat.
3. **Phân mảnh và nhân bản dữ liệu (Chunking & Replication):** Chia file để phân tán tải và tăng độ sẵn sàng khi có nút rời mạng.
4. **Xử lý đồng thời và cân bằng tải (Concurrency & Load Balancing):** Nhiều luồng cùng tải/ghi bitfield dễ gây Race Condition; phân phối yêu cầu đều giữa các Seeder.
5. **Chịu lỗi và khôi phục (Fault Tolerance & Recovery):** Peer sập nguồn đột ngột giữa chừng (Crash failure), mất kết nối tạm thời.
6. **Toàn vẹn dữ liệu trong môi trường không tin cậy (Integrity / Malicious Peer):** Chống dữ liệu lỗi đường truyền hoặc Peer độc hại gửi chunk giả mạo.

---

## 6. Phân tích và lựa chọn giải pháp (Analysis and Solution Selection) *(~2 trang)*
### 6.1. So sánh và lựa chọn kiến trúc P2P
- Lập bảng so sánh 3 mô hình: **Client-Server thuần** vs **Hybrid P2P (Tracker + Direct P2P)** vs **Pure P2P (DHT/Kademlia)**.
- Lý do chọn **Hybrid P2P**: Tách biệt hoàn toàn **Control Plane** (Tracker quản lý metadata/peer list nhẹ) và **Data Plane** (Peer truyền thẳng file nhị phân qua TCP), dễ kiểm soát, tốc độ khám phá $O(1)$, đúng trọng tâm đề tài DS02.

### 6.2. Lựa chọn giao thức truyền tin
- **HTTP/REST (JSON)** cho giao tiếp Peer $\leftrightarrow$ Tracker: đơn giản, chuẩn hóa, dễ debug và tích hợp Web Dashboard.
- **Raw TCP Socket + Binary Framing** cho giao tiếp Peer $\leftrightarrow$ Peer: giảm overhead so với HTTP, đảm bảo độ tin cậy truyền byte nhị phân (không mất gói, đúng thứ tự tầng vận chuyển).

### 6.3. Lựa chọn công nghệ & thư viện
- Bảng giải thích lựa chọn: Python 3, FastAPI + Uvicorn, Pydantic, `socket`, `struct`, `threading` / `concurrent.futures.ThreadPoolExecutor`, `hashlib` (SHA-256), Docker & Docker Compose, HTML/TailwindCSS (Web Dashboard).

---

## 7. Kiến trúc hệ thống (System Architecture) *(~2–2.5 trang)*
### 7.1. Sơ đồ kiến trúc tổng thể
- Vẽ sơ đồ phân tách rõ **Control Plane (HTTP Port 8000)** và **Data Plane (Direct TCP Port 6001, 6002, 6003, 6005...)** kèm chú thích: *Tracker không bao giờ lưu trữ hoặc chuyển tiếp dữ liệu file*.

### 7.2. Mô tả các thành phần chính
1. **Core Module (`src/core/`):** `metadata.py` (mô hình hóa file `.json`), `chunker.py` (`split_file`, `calculate_hash`, `verify_chunk`, `merge_chunks`).
2. **Tracker Server (`src/tracker/server.py`):** Điều phối danh bạ, lưu trạng thái `registered_files`, `file_peers`, `peer_heartbeats`, tự loại bỏ peer quá hạn timeout.
3. **Peer Node (`src/peer/client.py`, `src/peer/protocol.py`):** Thiết kế kép vừa làm **TCP Upload Server** (lắng nghe trên `0.0.0.0:port`) vừa làm **Multi-threaded Download Client**.
4. **Giao diện điều khiển & Trực quan hóa:**
   - **LAN CLI (`run_peer.py`):** Cho phép nhiều máy tính thật trong mạng LAN/Wi-Fi chia sẻ và tải file trực tiếp.
   - **Web Dashboard (`src/web/templates/index.html`, `src/web/scenario_runner.py`):** Hiển thị bản đồ mảnh (Piece Map), Live LAN Peers Monitor, log socket thời gian thực và 4 kịch bản demo.

---

## 8. Thiết kế các thành phần và giao thức (Component and Protocol Design) *(~3 trang)*
### 8.1. Cấu trúc dữ liệu Metadata (`FileMetadata`)
- Trình bày bảng các trường dữ liệu và ví dụ JSON thực tế sinh ra từ hệ thống.

### 8.2. Thiết kế REST API của Tracker (`src/tracker/server.py`)
- Lập bảng đặc tả các Endpoint:
  - `POST /register_file`: Đăng ký metadata file mới.
  - `POST /announce`: Peer báo danh `(peer_id, file_id, ip, port, bitfield)`.
  - `GET /peers/{file_id}`: Lấy danh sách peer đang online + metadata (tự động quét timeout).
  - `POST /heartbeat`: Duy trì trạng thái sống của Peer.
  - `GET /files` & `GET /active_peers`: Tra cứu toàn mạng cho CLI và Live Monitor.

### 8.3. Thiết kế Giao thức Nhị phân TCP Framing (`src/peer/protocol.py`)
- Vẽ sơ đồ cấu trúc gói tin:
  ```text
  +--------------------+--------------------+----------------------+-----------------------+
  | header_len (4B !I) |  data_len (4B !I)  | JSON Header (UTF-8)  | Binary Payload (Bytes)|
  +--------------------+--------------------+----------------------+-----------------------+
  ```
- Trình bày định dạng gói tin `REQUEST_CHUNK` và phản hồi `SUCCESS` / `ERROR`.
- Giải thích cơ chế `recv_exact(sock, num_bytes)` để xử lý hiện tượng phân mảnh gói TCP trên mạng.

### 8.4. Biểu đồ tuần tự (Sequence Diagrams)
- Sơ đồ 1: Quy trình Seeding & Khám phá Peer.
- Sơ đồ 2: Quy trình Tải song song & Vừa tải vừa chia sẻ (Seed-while-Leeching).
- Sơ đồ 3: Quy trình Phát hiện lỗi (Timeout / Sai SHA-256) & Failover.

---

## 9. Định danh và khám phá nút mạng (Identification and Discovery) *(~1.5 trang)*
- **Cơ chế định danh:** `peer_id` duy nhất (ví dụ `PEER_<ip_octet>_<port>`), `file_id` rút gọn từ UUID/MD5, bộ định danh mạng `(ip, port)`.
- **Phát hiện địa chỉ mạng LAN tự động (`get_local_ip()`):** Giải thích cách socket UDP route để tìm IP LAN thật (`192.168.x.x`) làm địa chỉ quảng bá, đồng thời bind server trên `0.0.0.0` để nhận kết nối cả localhost và mạng ngoài.
- **Vòng đời Peer trên Tracker:** Đăng ký $\rightarrow$ Announce bitfield $\rightarrow$ Heartbeat mỗi 5 giây $\rightarrow$ Quét hết hạn `PEER_TIMEOUT_SECONDS = 15s` khi truy vấn `/peers/{file_id}`.

---

## 10. Xử lý đồng thời, đồng bộ và tính nhất quán (Concurrency, Synchronization, and Consistency) *(~2 trang)*
### 10.1. Kiến trúc đa luồng phía Server và Client
- **Phía Upload Server:** Luồng `_listen_loop` gọi `accept()` và tạo luồng daemon `_handle_client` riêng cho từng kết nối đến $\rightarrow$ phục vụ đồng thời nhiều Leecher.
- **Phía Download Client:** `ThreadPoolExecutor(max_workers=4)` tải song song nhiều chunk cùng lúc, giới hạn số kết nối đồng thời tránh quá tải.
- **Thuật toán lập lịch cân bằng tải:** Phân bổ chunk cho các Seeder theo nguyên tắc chọn peer đang gán ít việc nhất (`peer_assignment_counters`, Round-Robin Load Balancing).

### 10.2. Xung đột và cơ chế đồng bộ hóa
- **Xung đột có thể xảy ra:** Nhiều luồng tải xong cùng lúc đồng thời cập nhật tập hợp `self.owned_chunks[file_id]` và gọi `announce_to_tracker()`.
- **Giải pháp:** Sử dụng `threading.Lock()` (`self._chunks_lock`) bảo vệ các vùng găng (critical sections) khi đọc/ghi bitfield; mỗi chunk ghi ra một file độc lập `chunk_{idx}.bin` nên không xảy ra tranh chấp ghi đè file giữa các luồng.

### 10.3. Mô hình nhất quán (Consistency Model)
- **Chunk bất biến (Immutable Data):** Mỗi chunk được cố định bằng mã SHA-256, đảm bảo tính toàn vẹn tuyệt đối.
- **Ghép file theo thứ tự:** Dù các chunk tải xong bất đồng bộ (out-of-order), `merge_chunks()` luôn ghép tuần tự từ `0` đến `num_chunks - 1`.
- **Eventual Consistency trên Tracker:** Bitfield của các peer được cập nhật ngay sau mỗi chunk hoàn tất (`auto_announce=True`), dần hội tụ về trạng thái mới nhất trên toàn mạng.

---

## 11. Chịu lỗi và khôi phục (Fault Tolerance and Recovery) *(~2 trang)*
### 11.1. Mô hình lỗi (Failure Model)
- Lỗi sập tiến trình / ngắt mạng đột ngột (Crash / Omission failure).
- Lỗi tắc nghẽn / phản hồi chậm (Timeout).
- Lỗi hỏng hoặc bị giả mạo dữ liệu (Corrupted / Byzantine chunk).

### 11.2. Cơ chế phát hiện lỗi (Failure Detection)
- Trên Tracker: Cơ chế **Heartbeat & Lease Timeout (15 giây)** tự động gỡ bỏ Peer chết khỏi danh sách trả về.
- Trên Peer Client: **Socket Timeout (2–3 giây)** bắt các ngoại lệ `socket.timeout`, `ConnectionRefusedError`, `ConnectionResetError`, `OSError`.

### 11.3. Cơ chế khôi phục và chuyển nguồn (Failover & Resume)
- **Failover Engine (`download_chunk_with_failover`):** Mỗi chunk được lập danh sách `ordered_candidates = [primary_peer, backup_peer_1, ...]`. Nếu nguồn chính thất bại (sập nguồn hoặc gửi sai hash), hệ thống tự động chuyển sang nguồn dự phòng tiếp theo mà không làm gián đoạn quá trình tải.
- **Resume / Chống tải trùng lặp:** Trước khi lập lịch tải, Peer tính `missing_chunks = [i for i in range(num_chunks) if i not in current_owned]` và chỉ tải phần còn thiếu.

---

## 12. Bảo mật và tính toàn vẹn dữ liệu (Security) *(~1–1.5 trang)*
- **Xác thực toàn vẹn 2 lớp bằng SHA-256:**
  - *Lớp 1 (Per-chunk):* `verify_chunk(chunk_data, expected_hash)` phát hiện sai lệch dù chỉ 1 bit (hiệu ứng tuyết lở), loại bỏ ngay dữ liệu từ Rogue Peer.
  - *Lớp 2 (End-to-end file):* `merge_chunks()` tính lại SHA-256 toàn bộ file sau khi ghép và đối chiếu với `metadata.file_hash`.
- **Kiểm tra và làm sạch dữ liệu đầu vào (Input Validation):** Sử dụng Pydantic Models trên FastAPI để xác thực kiểu dữ liệu request; kiểm tra sự tồn tại của `file_id` và `peer_id` trước khi xử lý.
- **Bảo vệ tài nguyên & mã nguồn:** Giới hạn số luồng tải (`max_threads`), không lưu mật khẩu hay khóa bí mật trên GitHub (sử dụng `.env.example` và `.gitignore`).

---

## 13. Cài đặt và triển khai (Implementation) *(~2.5–3 trang)*
### 13.1. Tổ chức mã nguồn
- Sơ đồ cây thư mục dự án (`src/`, `tests/`, `deploy/`, `scripts/`, `docs/`, `run_peer.py`, `requirements.txt`, `README.md`).

### 13.2. Chi tiết cài đặt các thuật toán tiêu biểu
- Trích dẫn ngắn gọn và giải thích logic các hàm cốt lõi: `split_file()`, `send_packet()`/`recv_exact()`, `download_chunk_with_failover()`, `download_file_parallel()`.

### 13.3. Triển khai đa môi trường
1. **Triển khai đa tiến trình cục bộ:** Dùng `scripts/run_all.bat` và nhiều cửa sổ `run_peer.py --port 6001/6002`.
2. **Triển khai nhiều máy tính qua mạng LAN:** Tracker chạy `--host 0.0.0.0`, các máy khác chạy `run_peer.py --tracker http://<IP_LAN>:8000`.
3. **Triển khai bằng Docker Compose (`deploy/docker-compose.yml`):** 1 container Tracker + 3 container Peer độc lập giao tiếp qua mạng ảo `p2p_net`.
4. **Giao diện Web Dashboard (`http://localhost:8000/dashboard`):** Chụp ảnh giao diện Piece Map, Live LAN Monitor và giải thích cơ chế gọi API thực thi Socket thật.

---

## 14. Kiểm thử và kết quả đạt được (Testing and Results) *(~3 trang)*
*(Đảm bảo đủ số lượng bài test bắt buộc tại Mục 9: $\ge 8$ Functional, $\ge 2$ Concurrency, $\ge 2$ Fault/Recovery, $\ge 2$ Security/Invalid-data, $\ge 1$ Performance)*

### 14.1. Kế hoạch và ma trận kiểm thử (Test Matrix)
Lập bảng tổng hợp gồm: `Test ID | Nhóm | Tên kịch bản | File test / Công cụ | Kết quả kỳ vọng | Kết quả thực tế`.
- **Nhóm 1 – Kiểm thử chức năng (8+ tests):**
  - `FT-01`: Cắt file thành chunks 512 KB và sinh SHA-256 (`test_step1.py`).
  - `FT-02`: Lưu và nạp lại file Metadata JSON (`test_step1.py`).
  - `FT-03`: Ghép file theo thứ tự và đối chiếu file hash (`test_step1.py`).
  - `FT-04`: Đăng ký file mới lên Tracker `/register_file` (`test_step2.py`).
  - `FT-05`: Peer báo danh bitfield `/announce` & tra cứu `/peers/{file_id}` (`test_step2.py`).
  - `FT-06`: Gửi Heartbeat duy trì trạng thái sống (`test_step2.py`).
  - `FT-07`: Truyền file nhị phân trực tiếp 1-1 qua TCP Socket bỏ qua Tracker (`test_step3.py`).
  - `FT-08`: Truyền file qua mạng LAN bằng `run_peer.py` / `test_lan_peers.py` và upload file thực tế qua Web Dashboard.
- **Nhóm 2 – Kiểm thử đồng thời (2+ tests):**
  - `CT-01`: Tải song song 4 luồng đồng thời từ 2 Seeders (`test_step4.py`).
  - `CT-02`: Vừa tải vừa upload – Peer 3 mới có 25% file đã phục vụ upload Chunk 0 cho Peer 4 song song với việc tải tiếp (`test_step4.py`, `test_step5.py`).
- **Nhóm 3 – Kiểm thử chịu lỗi & khôi phục (2+ tests):**
  - `FR-01`: Tracker tự phát hiện và gỡ bỏ Peer quá hạn Heartbeat 15s (`test_step2.py`).
  - `FR-02`: Peer 1 sập nguồn đột ngột khi đang tải $\rightarrow$ Peer 3 tự Failover sang Peer 2 hoàn tất 100% file (`test_step5.py` & Web Kịch bản 2).
  - `FR-03`: Kiểm thử Resume/Idempotency – tải lại file đã có đủ chunks không tải thừa qua mạng.
- **Nhóm 4 – Kiểm thử bảo mật & dữ liệu không hợp lệ (2+ tests):**
  - `ST-01`: Rogue Peer cố tình gửi Chunk 1 bị chỉnh sửa dữ liệu $\rightarrow$ Leecher phát hiện sai SHA-256, loại bỏ và tải lại từ Peer chuẩn (`test_step1.py`, `test_step5.py`).
  - `ST-02`: Truy vấn `file_id` không tồn tại (HTTP 404) hoặc gửi `/heartbeat` cho peer chưa announce (HTTP 400) bị Tracker từ chối hợp lệ.
- **Nhóm 5 – Thử nghiệm hiệu năng (1+ experiment):**
  - `PT-01`: Đo và so sánh thời gian tải & tốc độ (MB/s) giữa tải 1 nguồn tuần tự vs tải song song đa nguồn trên các kích thước file khác nhau (2 MB, 6.5 MB, 20 MB...).

### 14.2. Bằng chứng thực thi (Screenshots & Logs)
- Chèn ảnh chụp màn hình terminal chạy `scripts/run_tests.bat` (5/5 PASSED).
- Chèn ảnh chụp `Get-NetTCPConnection` / `netstat` chứng minh cổng 6001, 6002, 8000 mở thật trên hệ điều hành.
- Chèn ảnh chụp Web Dashboard cho 7 kịch bản bắt buộc (Mục 6.4).

---

## 15. Đánh giá hiệu năng (Performance Evaluation) *(~1.5–2 trang)*
### 15.1. Môi trường thực nghiệm
- Cấu hình phần cứng (CPU, RAM, OS Windows), phiên bản Python, môi trường mạng (Loopback và Wi-Fi LAN).

### 15.2. Các chỉ số đo đạc và phân tích kết quả
*(Thực nghiệm chạy đo lấy số liệu thực tế và lập bảng + vẽ biểu đồ cột/đường)*
1. **Thời gian tải (Download Time) & Thông lượng (Throughput - MB/s):** So sánh giữa `max_threads = 1` (tuần tự, 1 Seeder) vs `max_threads = 2, 4` (song song, 2 Seeders) trên 3 cỡ file (ví dụ: 2 MB, 6.5 MB, 20 MB).
2. **Thời gian phát hiện lỗi & Chuyển nguồn (Failure Detection & Failover Time):** Đo thời gian từ lúc Peer 1 ngắt socket tới lúc Peer 3 chuyển sang Peer 2 thành công (thường $< 0.1\text{s}$ với `ConnectionRefusedError` và $\approx 2.0\text{s}$ với `Socket Timeout`).
3. **Chi phí băm SHA-256 & Kích thước Chunk:** Đánh giá sự đánh đổi khi chọn chunk size 64 KB vs 256 KB vs 512 KB (chunk quá nhỏ tăng overhead giao thức/HTTP announce; chunk quá lớn làm giảm độ mịn khi tải song song và tốn thời gian tải lại nếu lỗi).

---

## 16. Hạn chế và hướng phát triển (Limitations and Future Work) *(~1 trang)*
### 16.1. Hạn chế hiện tại
- Tracker vẫn là điểm tập trung (Single Point of Failure nếu Tracker sập *trước khi* Leecher lấy được danh sách peer) và lưu trạng thái trên RAM (mất dữ liệu nếu khởi động lại Tracker).
- Chưa hỗ trợ NAT Traversal (STUN/TURN) cho môi trường Internet/WAN phức tạp.
- Thuật toán chọn chunk đang theo thứ tự tuần tự kết hợp cân bằng tải đơn giản, chưa tối ưu theo Rarest-First.
- Kênh truyền Socket TCP chưa được mã hóa TLS.

### 16.2. Hướng phát triển (Advanced Features – theo Mục 6.5)
- Thay thế/bổ sung Tracker bằng mạng phân tán **DHT (Kademlia)** hoặc **Replicated Tracker** lưu trạng thái trên Redis/Database.
- Cài đặt thuật toán **Rarest-First** (ưu tiên tải mảnh hiếm nhất) và **Tit-for-Tat / Choking-Unchoking** chống hiện tượng "ăn bám" (Free-riding).
- Bổ sung mã hóa kênh truyền **TLS/SSL**, **Magnet Links**, và giới hạn băng thông (**Bandwidth Limiting**).

---

## 17. Kết luận (Conclusion) *(~0.5–1 trang)*
- Tổng kết những kết quả đã đạt được so với mục tiêu đề ra ở Mục 1 và Mục 6.
- Nhấn mạnh những kiến thức thực tiễn rút ra về môn Hệ thống phân tán: giao tiếp mạng cấp thấp, lập trình đa luồng, đồng bộ hóa, xử lý lỗi và bảo đảm tính toàn vẹn dữ liệu.

---

## 18. Tài liệu tham khảo (References) *(~0.5 trang)*
1. B. Cohen, *"The BitTorrent Protocol Specification (BEP 0003)"*, BitTorrent.org, 2008.
2. M. van Steen & A. S. Tanenbaum, *"Distributed Systems"*, 4th Edition, 2023.
3. FastAPI Documentation – https://fastapi.tiangolo.com/
4. Python Standard Library (`socket`, `struct`, `hashlib`, `threading`, `concurrent.futures`) – https://docs.python.org/3/
5. Docker & Docker Compose Documentation – https://docs.docker.com/

---

## 19. Phụ lục (Appendices) *(Không giới hạn trang)*
- **Phụ lục A:** Đường dẫn GitHub Repository, hướng dẫn cài đặt môi trường và khởi chạy từng bước (`Local`, `LAN`, `Docker`).
- **Phụ lục B:** Ảnh chụp toàn bộ 7 kịch bản demo bắt buộc (Metadata, Song song, Vừa tải vừa upload, Sập nguồn, Chuyển nguồn, Phát hiện chunk hỏng, Ghép file & Verify Hash).
- **Phụ lục C:** Bảng nhật ký kiểm thử và lịch sử đóng góp (GitHub Commit Log).
