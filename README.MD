# Dự báo mực nước và nguy cơ ngập lụt từ dữ liệu cảm biến IoT

Bài tập lớn môn **Big Data**. Hệ thống giả lập luồng dữ liệu từ các cảm biến mực nước, nâng chất lượng dữ liệu bằng **Kalman filter**, phát hiện cảm biến hỏng, dự báo mực nước và nguy cơ ngập ngày hôm sau, rồi phát cảnh báo sớm và hiển thị trên dashboard theo thời gian thực.

> Tài liệu quy định dữ liệu trao đổi giữa các thành phần: **[CONTRACT.md](CONTRACT.md)**. Đọc file này trước khi viết code.

---

## Mục lục

1. [Mục tiêu](#1-mục-tiêu)
2. [Dữ liệu](#2-dữ-liệu)
3. [Kiến trúc hệ thống](#3-kiến-trúc-hệ-thống)
4. [Cấu trúc thư mục](#4-cấu-trúc-thư-mục)
5. [Kiến thức cần nắm về các thành phần](#5-kiến-thức-cần-nắm-về-các-thành-phần)
6. [Cài đặt và khởi chạy](#6-cài-đặt-và-khởi-chạy)
7. [Dịch vụ và địa chỉ kết nối](#7-dịch-vụ-và-địa-chỉ-kết-nối)
8. [Lệnh thường dùng](#8-lệnh-thường-dùng)
9. [Xử lý sự cố thường gặp](#9-xử-lý-sự-cố-thường-gặp)
10. [Quy ước làm việc nhóm](#10-quy-ước-làm-việc-nhóm)

---

## 1. Mục tiêu

- **Thu thập dữ liệu** từ cảm biến mực nước: giả lập luồng dữ liệu IoT bằng Kafka.
- **Nâng chất lượng dữ liệu IoT** bằng Kalman filter (hiệu chỉnh cảm biến).
- **Phát hiện cảm biến hỏng** dựa trên innovation của Kalman filter.
- **Dự báo mực nước và nguy cơ ngập** ngày t+1 từ lượng mưa, gồm hai bài toán:
  - hồi quy mực nước: Persistence, Climatology, Linear Regression, GBT;
  - phân loại ngập / không ngập: Persistence, Logistic Regression, Random Forest;

  mỗi bài toán đánh giá theo hai kịch bản: **A** (chỉ dữ liệu quá khứ) và **B** (có thêm lượng mưa dự báo ngày t+1).
- **Cảnh báo sớm theo thời gian thực** qua topic Kafka `flood-alerts`.
- **Trực quan hóa** dữ liệu và kết quả trên dashboard.

---

## 2. Dữ liệu

Nguồn: ASIA_FLOOD_25YEAR_DATASET, file `station_daily_data_full.csv`: 50 trạm × 9.132 ngày (2000–2024), 10 lưu vực sông châu Á.

| Cột | Ý nghĩa |
|---|---|
| `station_id`, `station_name` | Mã và tên trạm quan trắc |
| `basin`, `country` | Lưu vực sông, quốc gia |
| `latitude`, `longitude` | Tọa độ trạm |
| `date` | Ngày quan trắc |
| `rainfall_mm` | Lượng mưa trong ngày (mm) |
| `river_level_m` | Mực nước sông (m), là giá trị "thật" để giả lập cảm biến |
| `soil_moisture_percent` | Độ ẩm đất (%) |
| `temperature_celsius` | Nhiệt độ (°C) |
| `monsoon_season` | Có thuộc mùa gió hay không (0/1) |
| `data_quality_score` | Điểm chất lượng dữ liệu của trạm |
| `flood_event_occurred` | **Nhãn ngập** (0/1), khoảng 2,2% số ngày |

---

## 3. Kiến trúc hệ thống

```
CSV station_daily_data_full.csv  (dữ liệu "thật")
      │
      ▼
[Producer Python]  ← thêm nhiễu, gai, mất gói, lỗi cảm biến (seed 42)
      │   JSON: station_id, event_time, sent_at, rainfall_mm,
      │         rainfall_forecast_next, soil_moisture_percent, water_level_m
      │   (nhãn lỗi ghi riêng ra file ground truth, KHÔNG gửi kèm bản tin)
      ▼
[Kafka]  topic: sensor-raw  (key = station_id, 5 partition)
      │
      ▼
[Spark Structured Streaming]
      ├─ Parse + kiểm tra schema           → Bronze (dữ liệu thô)
      ├─ Kalman filter theo từng trạm      → Silver (dữ liệu đã lọc + cờ cảm biến hỏng)
      └─ Đặc trưng + mô hình dự báo        → Gold (dự báo, xác suất ngập)
               └─ xác suất > ngưỡng ─────→ [Kafka] topic: flood-alerts
      │
      ▼
[PostgreSQL / Parquet]  →  [Dashboard: Grafana]
```

| Thành phần | Vai trò |
|---|---|
| Producer (Python) | Đọc CSV, gắn nhiễu và lỗi cảm biến, gửi bản tin JSON vào Kafka theo nhịp tăng tốc (ví dụ 1 giây = 1 ngày) |
| Kafka | Tầng tiếp nhận: tách rời nguồn và nơi xử lý, làm bộ đệm, giữ thứ tự theo trạm, cho phép phát lại |
| Spark Structured Streaming | Xử lý luồng: kiểm tra schema, Kalman filter, tạo đặc trưng, dự báo, phát cảnh báo |
| PostgreSQL | Lưu kết quả để dashboard truy vấn |
| Parquet (Bronze / Silver / Gold) | Kho dữ liệu dạng file, chia partition theo lưu vực |
| Grafana | Dashboard: bản đồ trạm, mực nước thô/lọc/dự báo, cảnh báo, trạng thái cảm biến |

---

## 4. Cấu trúc thư mục

```
bigdata-floodPrediction/
├── docker-compose.yml        # toàn bộ hạ tầng
├── .env                      # mật khẩu, cổng — KHÔNG đưa lên Git
├── .env.example              # mẫu của .env
├── CONTRACT.md               # hợp đồng giao diện giữa các thành phần
├── README.md
├── requirements.txt          # thư viện Python của dự án
├── data/                     # KHÔNG đưa lên Git
│   ├── raw/                  # CSV gốc, stations_clean.csv
│   ├── noisy/                # dữ liệu nhiễu + ground truth (Parquet)
│   └── lake/                 # bronze / silver / gold (Parquet), _checkpoints/ của Spark Streaming
├── infra/
│   ├── postgres/init/        # script SQL tạo bảng (chạy khi khởi tạo database)
│   ├── spark/                # Dockerfile + spark-defaults.conf của image Spark
│   └── grafana/              # cấu hình Grafana
├── common/                   # code dùng chung: schema, module nhiễu
├── producer/                 # Producer và script thử Kafka          (TV1)
├── streaming/                # job Spark (batch + Structured Streaming) (TV1, nhúng Kalman của TV2)
├── notebooks/                # EDA, Kalman                           (TV2)
└── ml/                       # đặc trưng, huấn luyện, đánh giá mô hình (TV3)
```

---

## 5. Kiến thức cần nắm về các thành phần

### 5.1. Docker và Docker Compose

| Khái niệm | Nội dung |
|---|---|
| Container và máy ảo | Container là tiến trình được cô lập, **dùng chung nhân hệ điều hành**, nên nhẹ và khởi động nhanh hơn máy ảo. Trên Windows, Docker Desktop chạy mọi container trong **một** máy ảo Linux nhẹ (WSL 2); RAM và CPU do WSL 2 quản lý (`.wslconfig`) |
| Docker Compose | Mô tả toàn bộ hạ tầng trong một file (`docker-compose.yml`) và khởi động bằng một lệnh. Các dịch vụ nằm chung một **mạng nội bộ** và gọi nhau bằng **tên dịch vụ** (`kafka`, `postgres`) |
| Biến môi trường `.env` | Compose tự đọc `.env` và thay `${TÊN}` trong `docker-compose.yml`. `.env` chỉ chứa dòng `TÊN=GIÁ_TRỊ`; mật khẩu nằm ở đây để không lộ lên Git |
| Ánh xạ cổng `"HOST:CONTAINER"` | Vế trái là cổng trên máy, vế phải là cổng bên trong container. Container khác luôn dùng cổng bên phải |
| Healthcheck và `depends_on` | Healthcheck cho Docker biết dịch vụ đã **thực sự sẵn sàng** (không chỉ "đã chạy"); `depends_on: condition: service_healthy` bắt dịch vụ khác chờ đến lúc đó |
| Init container | Container chạy một lần để chuẩn bị (ví dụ `kafka-init` tạo topic) rồi thoát với mã `0`. Lệnh viết idempotent (`--if-not-exists`) nên chạy lại nhiều lần vẫn an toàn |
| Named volume và bind mount | **Named volume** (`pg_data`) do Docker quản lý, sống lâu hơn container, dùng cho dữ liệu. **Bind mount** (`./infra/...`) trỏ vào thư mục của dự án, dùng cho file cấu hình/script quản lý bằng Git |
| Giới hạn tài nguyên | `deploy.resources.limits.memory` giới hạn RAM từng container, để một dịch vụ không chiếm hết bộ nhớ của các dịch vụ khác |
| Dockerfile, image, container | **Dockerfile** là công thức tạo image (image nền + thư viện cần cài); **image** là thành phẩm đóng gói, bất biến (`docker compose build`); **container** là image đang chạy (`docker compose up`). `docker-compose.yml` chỉ quyết định *chạy những container nào, nối với nhau ra sao*; muốn thay đổi *bên trong* container (cài thêm thư viện) phải dùng Dockerfile. Một image chạy được thành nhiều container (`spark-master`, `spark-worker`) |
| Quyền của named volume | Khi một named volume **rỗng** được gắn vào thư mục đã có trong image, Docker sao chép cả nội dung lẫn quyền truy cập của thư mục đó. Vì vậy thư mục `/tmp/.ivy2` được tạo sẵn (quyền ghi cho user `spark`) trong Dockerfile |

### 5.2. Kafka

| Khái niệm | Nội dung |
|---|---|
| Vai trò so với database | Kafka là **nhật ký sự kiện có thứ tự**: tiếp nhận tốc độ cao, làm bộ đệm khi lưu lượng tăng đột biến, cho nhiều bên đọc độc lập và cho phép **phát lại**. Database lưu **trạng thái/kết quả** để truy vấn. Hai thành phần bổ sung chứ không thay thế nhau |
| Broker, controller, KRaft | Broker lưu và phân phối bản tin; controller quản lý cụm. Chế độ **KRaft** cho Kafka tự làm controller, không cần ZooKeeper. Cụm của dự án có 1 nút đóng cả hai vai |
| Listener | `INTERNAL` (`kafka:9092`) cho container trong Docker; `EXTERNAL` (`localhost:9094`) cho chương trình trên máy; `CONTROLLER` (9093) dùng nội bộ. Kafka trả về **địa chỉ quảng bá** cho client, nên mỗi phía cần một địa chỉ riêng |
| Topic, partition | Topic chia thành nhiều partition để xử lý song song. Số partition quyết định mức song song tối đa khi đọc |
| Key và thứ tự | Bản tin **cùng key** luôn vào **cùng partition** và giữ thứ tự **trong partition**. Kafka **không** giữ thứ tự giữa các partition. Key = `station_id` để Kalman nhận dữ liệu từng trạm đúng thứ tự |
| Producer | `produce()` chỉ đưa bản tin vào hàng đợi và gửi theo lô (batching); `flush()` chờ mọi bản tin được xác nhận; callback `on_delivery` cho biết bản tin đã tới nơi chưa |
| Consumer group, offset, lag | Mỗi nhóm tự ghi nhớ **offset** đã đọc tới; khởi động lại thì đọc tiếp từ đó. **Lag** = số bản tin còn chưa đọc; lag tăng liên tục nghĩa là bên xử lý không theo kịp |
| `__consumer_offsets` | Topic nội bộ (mặc định 50 partition) nơi Kafka lưu offset của các nhóm; tạo tự động khi có nhóm đầu tiên |
| Replication | Cụm 1 broker nên replication = 1, không có dự phòng. Cụm thật thường dùng 3 broker, replication 3 |
| Schema | Kafka không kiểm tra nội dung bản tin (chỉ là byte). Schema được thực thi ở hai đầu: Producer kiểm tra trước khi gửi, Spark tách bản tin lỗi vào vùng cách ly |

### 5.3. PostgreSQL

| Khái niệm | Nội dung |
|---|---|
| Script khởi tạo | File `.sql` trong `/docker-entrypoint-initdb.d` tự chạy **chỉ khi database được tạo lần đầu** (volume rỗng). User, mật khẩu, tên database cũng chỉ được tạo lúc đó |
| Kiểu dữ liệu | `TIMESTAMPTZ`: thời điểm tuyệt đối theo UTC. `DOUBLE PRECISION`: số đo gần đúng, nhanh và gọn hơn `NUMERIC` |
| Khóa chính ghép | (`station_id`, `event_time`) là khóa tự nhiên: một trạm, một thời điểm chỉ có một dòng |
| Khóa ngoại | Chỉ chấp nhận trạm đã có trong `stations`: phải nạp `stations` trước, xóa bảng con trước bảng cha |
| `CHECK` | Chặn giá trị vô lý ngay tại database (mưa âm, xác suất ngoài [0, 1]…) |
| Chỉ mục | Tăng tốc truy vấn nhưng làm chậm việc ghi; chỉ tạo khi có truy vấn thật cần. **Partial index** chỉ chứa một phần dòng (ví dụ các dòng có lỗi) nên rất nhỏ |
| Ghi idempotent | Spark Streaming đảm bảo "ít nhất một lần" nên có thể xử lý lại một lô. Ghi bằng `INSERT ... ON CONFLICT DO NOTHING` để ghi lại bao nhiêu lần kết quả vẫn như một lần |

### 5.4. Luồng dữ liệu và xử lý

| Khái niệm | Nội dung |
|---|---|
| `event_time` và `sent_at` | `event_time`: thời điểm đo, dùng cho xử lý theo thời gian và watermark. `sent_at`: thời điểm gửi, dùng để đo **độ trễ đầu–cuối** (`ingested_at − sent_at`) |
| Giả lập cảm biến IoT | `river_level_m` được coi là giá trị thật; Producer gắn nhiễu Gauss và các loại lỗi `spike`, `missing`, `dropout`, `stuck`, `drift` |
| Ground truth | Nhãn lỗi được ghi ra file riêng, **không gửi kèm bản tin**. Bộ lọc làm việc "mù"; ground truth chỉ dùng ở cuối để chấm điểm (RMSE, Precision/Recall), nối với kết quả theo `message_id` |
| Kalman filter | Vừa **làm sạch** (nhiễu, spike, mất gói) vừa **phát hiện bất thường** qua innovation (stuck, drift, dropout). Dùng lượng mưa làm biến điều khiển, nên chỉ gắn nhiễu cho mực nước |
| Bronze / Silver / Gold | Kiến trúc medallion: Bronze = dữ liệu thô; Silver = đã làm sạch, đã lọc; Gold = đã tổng hợp, dự báo, sẵn sàng cho dashboard |
| Parquet và partition | Lưu theo cột, nén, giữ schema. `partitionBy("basin")` tạo thư mục con theo lưu vực; lọc theo lưu vực thì Spark chỉ đọc đúng thư mục đó (**partition pruning**) |

### 5.5. Spark

| Khái niệm | Nội dung |
|---|---|
| Spark và Kafka | Hai hệ thống **độc lập**: Kafka truyền và lưu bản tin, Spark **tính toán**. Spark đọc Kafka như một client (qua connector `spark-sql-kafka`), ngang hàng với đọc CSV, Parquet hay PostgreSQL |
| Driver, master, worker, executor | **Driver** chạy code của bạn, lập kế hoạch và chia task; **master** quản lý tài nguyên của cụm; **worker** cung cấp CPU/RAM; **executor** (do worker khởi động) là tiến trình thực sự xử lý dữ liệu |
| Lazy evaluation | Transformation (`filter`, `withColumn`, `groupBy`…) chỉ ghi lại kế hoạch; chỉ **action** (`count`, `show`, `write`…) mới khiến cụm chạy. Mỗi action tính lại từ đầu nếu không `persist()`/`cache()` |
| Partition và task | **Số task của một stage = số partition** của dữ liệu ở stage đó. **Số task chạy cùng lúc = tổng số nhân** của các executor; task nhiều hơn thì chạy thành nhiều lượt. Đọc topic Kafka: mỗi partition Kafka thành một task |
| Shuffle và stage | `groupBy`, `join`, `orderBy` cần dồn dữ liệu cùng khóa về một chỗ (**shuffle**); mỗi lần shuffle chia job thành một stage mới. Mặc định kết quả shuffle chia thành 200 partition (`spark.sql.shuffle.partitions`) |
| Gom sơ bộ (partial aggregation) | Trước khi shuffle, mỗi task tự gom nhóm phần dữ liệu của mình (ví dụ mang theo cặp count/sum để tính trung bình), nên lượng dữ liệu phải chuyển giảm rất nhiều |
| AQE | *Adaptive Query Execution* (bật mặc định ở Spark 3.5): sau mỗi stage, Spark nhìn kích thước thực tế và tự gộp các partition nhỏ (ví dụ 200 → 1), tránh tạo nhiều task và file rỗng |
| Thư mục dùng chung | Mỗi executor tự ghi phần dữ liệu của mình, nên master và worker phải thấy cùng một thư mục, cùng đường dẫn (`/opt/app/data`). Cụm thật dùng HDFS hoặc S3 cho vai trò này |
| Cấp phát tài nguyên | Ở chế độ standalone, mỗi ứng dụng mặc định **chiếm hết số nhân còn trống** và giữ đến khi kết thúc; ứng dụng sau phải chờ. Giới hạn bằng `spark.cores.max` |
| Structured Streaming | `readStream` đọc liên tục; dữ liệu mới được gom thành **micro-batch** theo `trigger` (ví dụ 5 giây). `foreachBatch` giao mỗi micro-batch cho một hàm như DataFrame thường, nên dùng được mọi lệnh ghi batch |
| Checkpoint | Thư mục lưu offset đã xử lý của job streaming. Khởi động lại thì đọc tiếp, không lặp; `startingOffsets` chỉ có tác dụng ở lần chạy đầu tiên (khi chưa có checkpoint). Spark không lưu offset vào consumer group của Kafka |
| Exactly-once | Checkpoint **cộng với** ghi idempotent (khóa chính + `ON CONFLICT DO NOTHING`) mới bảo đảm mỗi bản tin được ghi đúng một lần, kể cả khi xử lý lại |
| Báo cáo tiến độ streaming | Mỗi micro-batch có `numInputRows`, `inputRowsPerSecond`, `processedRowsPerSecond`, `durationMs` (`addBatch`, `commitOffsets`…): nguồn số liệu thông lượng và độ trễ. Xử lý nhanh hơn tốc độ dữ liệu đến thì hệ thống theo kịp |

---

## 6. Cài đặt và khởi chạy

### 6.1. Yêu cầu phần mềm và kiểm tra phiên bản

| Phần mềm | Yêu cầu | Lệnh kiểm tra (PowerShell) |
|---|---|---|
| WSL 2 | Đã bật | `wsl --version` |
| Docker Desktop | Backend WSL 2, có `docker compose` v2 | `docker --version`<br>`docker compose version` |
| RAM cho Docker | Tối thiểu **6–8 GB** | `(docker info --format "{{.MemTotal}}") / 1GB` |
| CPU cho Docker | Mặc định dùng toàn bộ nhân | `docker info --format "{{.NCPU}}"` |
| Git | Bất kỳ | `git --version` |
| Python | **3.11** (PySpark 3.5 chưa hỗ trợ bản mới hơn) | `py --list` |

Nếu RAM cấp cho Docker thấp hơn 6 GB, tạo file `%USERPROFILE%\.wslconfig`:

```ini
[wsl2]
memory=8GB
```

rồi chạy `wsl --shutdown` và mở lại Docker Desktop.

### 6.2. Chuẩn bị dự án

**1. Lấy mã nguồn và dữ liệu**

```powershell
git clone <đường-dẫn-repo>
cd bigdata-floodPrediction
```

Đặt file `station_daily_data_full.csv` vào `data/raw/`.

**2. Tạo file `.env`**

```powershell
Copy-Item .env.example .env
docker run --rm apache/kafka:3.9.0 /opt/kafka/bin/kafka-storage.sh random-uuid
```

Điền chuỗi vừa sinh vào `KAFKA_CLUSTER_ID`, đặt mật khẩu PostgreSQL. Nếu cổng mặc định đã bị chiếm (`netstat -ano | findstr :5433`), đổi `POSTGRES_HOST_PORT`.

**3. Tạo môi trường Python**

```powershell
py -3.11 -m venv venv
.\venv\Scripts\Activate.ps1
python --version            # phải là 3.11.x
pip install -r requirements.txt
```

Nếu PowerShell chặn chạy script: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.

### 6.3. Hạ tầng được xây dựng

Toàn bộ hạ tầng chạy trong Docker, mô tả trong `docker-compose.yml`:

| Container | Image | Bản chất | Lưu trữ |
|---|---|---|---|
| `kafka` | `apache/kafka:3.9.0` | Cụm Kafka **1 nút**, chế độ KRaft (vừa broker vừa controller); 3 listener; tắt tự tạo topic; heap giới hạn 512 MB | Không có volume: dữ liệu mất khi xóa container (có thể phát lại từ CSV) |
| `kafka-init` | `apache/kafka:3.9.0` | Init container: chờ Kafka healthy, tạo 3 topic rồi tự thoát | — |
| `kafka-ui` | `provectuslabs/kafka-ui:v0.7.2` | Giao diện web quản trị Kafka; RAM giới hạn 512 MB | — |
| `postgres` | `postgres:16` | Database kết quả, múi giờ UTC; tự tạo 5 bảng từ `infra/postgres/init/01_schema.sql` ở lần khởi tạo đầu; RAM giới hạn 512 MB | Named volume `pg_data`: dữ liệu giữ lại khi tắt hệ thống |
| `spark-master` | `floodprediction/spark:3.5.3` (build từ `infra/spark/Dockerfile`) | Master của cụm Spark standalone; đồng thời là nơi chạy **driver** khi `spark-submit`; nhận biến `POSTGRES_*`; RAM giới hạn 1,5 GB | Bind mount `streaming/`, `common/`, `data/`; named volume `spark_ivy` (lưu đệm connector) |
| `spark-worker` | `floodprediction/spark:3.5.3` | Worker đăng ký với `spark://spark-master:7077`, cung cấp **2 nhân, 2 GB** cho executor; RAM container giới hạn 2,5 GB | Bind mount `streaming/`, `common/`, `data/` (cùng đường dẫn với master) |

**Topic Kafka do `kafka-init` tạo**

| Topic | Key | Partition | Dùng cho |
|---|---|---|---|
| `sensor-raw` | `station_id` | 5 | Bản tin cảm biến. **Không gửi dữ liệu thử vào đây** |
| `flood-alerts` | `station_id` | 1 | Cảnh báo ngập |
| `sandbox` | tùy ý | 5 | Thử nghiệm |

**Bảng PostgreSQL**: `stations`, `sensor_readings`, `predictions`, `flood_alerts`, `pipeline_metrics`. Chi tiết cột, khóa, ràng buộc và chỉ mục: [CONTRACT.md](CONTRACT.md) mục 8.

**Image Spark riêng** (`infra/spark/Dockerfile`): lấy image chính thức làm nền, cài thêm `numpy`, `pandas`, `pyarrow` (bắt buộc cho `applyInPandas` của Kalman), `psycopg2-binary` (ghi `ON CONFLICT` vào PostgreSQL), `pyyaml`; chép `spark-defaults.conf` vào image. Chi tiết phiên bản và cấu hình mặc định: [CONTRACT.md](CONTRACT.md) mục 2.1, 2.4.

### 6.4. Khởi động hạ tầng

**Bước 1: kiểm tra cấu hình** (không khởi động gì, chỉ đọc file và thay biến từ `.env`):

```powershell
docker compose config
```

**Bước 2: build image Spark và khởi động**

```powershell
docker compose build
docker compose up -d
```

`build` chỉ cần chạy lần đầu và mỗi khi sửa `infra/spark/Dockerfile` hoặc `spark-defaults.conf`; lần đầu mất vài phút.

Thứ tự Docker Compose thực hiện:

1. Tạo mạng nội bộ `floodprediction-bigdata_default` và volume `pg_data` (nếu chưa có).
2. Khởi động `kafka` và `postgres`; mỗi dịch vụ tự kiểm tra sức khỏe (healthcheck).
3. Khi `kafka` **healthy**: khởi động `kafka-ui` và chạy `kafka-init` để tạo topic.
4. Nếu volume `pg_data` còn rỗng: `postgres` chạy script tạo bảng.
5. Khởi động `spark-master`, rồi `spark-worker` đăng ký với master.

**Bước 3: kiểm tra trạng thái**

```powershell
docker compose ps -a
```

| Container | Trạng thái đúng |
|---|---|
| `kafka` | `Up ... (healthy)` |
| `kafka-init` | `Exited (0)`: đã tạo topic xong và tự thoát |
| `kafka-ui` | `Up` |
| `postgres` | `Up ... (healthy)` |
| `spark-master` | `Up` |
| `spark-worker` | `Up` |

**Bước 4: kiểm tra từng thành phần**

```powershell
# Kafka: danh sách topic phải có flood-alerts, sandbox, sensor-raw
docker compose logs kafka-init

# Kafka từ máy (cổng EXTERNAL, topic sandbox)
python producer\kafka_smoke_test.py

# PostgreSQL: phải có 5 bảng
docker exec -it postgres psql -U <POSTGRES_USER> -d <POSTGRES_DB> -c "\dt"
```

Kafka UI: mở http://localhost:8085.

```powershell
# Spark: phiên bản và thư viện bên trong image
docker exec spark-master /opt/spark/bin/spark-submit --version
docker exec spark-master python3 -c "import numpy, pandas, pyarrow, psycopg2, yaml; print('ok')"

# Spark: chạy job thử trên cụm
docker exec spark-master /opt/spark/bin/spark-submit /opt/app/streaming/spark_smoke_test.py
```

Spark UI: mở http://localhost:8080, phải có **Alive Workers: 1**, *Cores 2, Memory 2.0 GiB*.

Lần đầu chạy job có đọc Kafka hoặc ghi PostgreSQL, Spark tải connector từ Internet (khoảng một phút); các lần sau dùng bộ nhớ đệm.

**Tắt hệ thống:** `docker compose down`. Dữ liệu Kafka mất (kể cả offset của các consumer group), dữ liệu PostgreSQL và bộ nhớ đệm connector giữ lại; lần `up` sau `kafka-init` tạo lại topic rỗng. Sau `down`/`up`, cần gửi lại dữ liệu vào Kafka trước khi thử các job đọc Kafka.

---

## 7. Dịch vụ và địa chỉ kết nối

| Dịch vụ | Image | Từ trong Docker | Từ máy (Windows) |
|---|---|---|---|
| Kafka (KRaft, 1 broker) | `apache/kafka:3.9.0` | `kafka:9092` | `localhost:9094` |
| Kafka UI | `provectuslabs/kafka-ui:v0.7.2` | — | http://localhost:8085 |
| PostgreSQL | `postgres:16` | `postgres:5432` | `localhost:${POSTGRES_HOST_PORT}` (mặc định 5433) |
| Spark master | `floodprediction/spark:3.5.3` | `spark://spark-master:7077` | http://localhost:8080 |
| Spark worker | `floodprediction/spark:3.5.3` | — | http://localhost:8081 |
| Ứng dụng Spark đang chạy | — | — | http://localhost:4040 (chỉ khi có job) |
| Grafana | *đang dựng* | | |

---

## 8. Lệnh thường dùng

### Docker Compose

| Lệnh | Tác dụng |
|---|---|
| `docker compose up -d` | Khởi động (chỉ tạo thêm phần còn thiếu) |
| `docker compose ps -a` | Xem trạng thái, kể cả container đã thoát |
| `docker compose logs -f <dịch-vụ>` | Xem log trực tiếp (`Ctrl+C` để thoát) |
| `docker compose down` | Tắt hệ thống. **Dữ liệu Kafka mất**, dữ liệu PostgreSQL **giữ lại** |
| `docker compose down -v` | Tắt và **xóa cả volume**: mất toàn bộ dữ liệu PostgreSQL. Chỉ dùng khi muốn làm lại từ đầu |

### Kafka

```powershell
# Danh sách topic
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

# Chi tiết một topic
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic sensor-raw

# Số bản tin trong từng partition
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic sensor-raw

# Đọc bản tin (kèm key và partition)
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic sandbox --from-beginning --max-messages 10 --property print.key=true --property print.partition=true

# Consumer group: offset đã đọc và lag
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --list
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group <tên-nhóm>
```

### PostgreSQL

```powershell
docker exec -it postgres psql -U <POSTGRES_USER> -d <POSTGRES_DB>
```

Trong `psql`: `\dt` (bảng), `\d <bảng>` (cấu trúc), `\di` (chỉ mục), `\q` (thoát). Khi kết quả dài hiện `(END)`, nhấn `q` để thoát màn hình phân trang.

Truy vấn có dấu ngoặc kép (ví dụ tên cột trùng từ khóa SQL) nên gõ **trong `psql` tương tác** thay vì `psql -c "..."`: PowerShell không dùng `\` làm ký tự thoát nên câu lệnh dễ bị cắt.

### Spark

```powershell
# Chạy một job (đường dẫn bên trong container)
docker exec spark-master /opt/spark/bin/spark-submit /opt/app/streaming/<file>.py

# Job có input() hoặc job streaming cần dừng bằng Ctrl+C
docker exec -it spark-master /opt/spark/bin/spark-submit /opt/app/streaming/<file>.py

# Ghi đè cấu hình cho một lần chạy
docker exec -it spark-master /opt/spark/bin/spark-submit --conf spark.cores.max=2 /opt/app/streaming/<file>.py

# Khởi động lại cụm Spark (giải phóng ứng dụng bị treo), không ảnh hưởng Kafka, PostgreSQL
docker compose restart spark-master spark-worker

# Log của master/worker
docker compose logs -f spark-worker
```

Job streaming: xóa thư mục `data/lake/_checkpoints/<tên-job>/` khi muốn đọc lại topic từ đầu (dữ liệu sẽ bị ghi lại nếu đích không idempotent).

---

## 9. Xử lý sự cố thường gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `failed to read .env: unexpected character ... in variable name` | Đặt nội dung YAML (như `ports:`) vào `.env` | `.env` chỉ chứa `TÊN=GIÁ_TRỊ`; cấu hình dịch vụ đặt trong `docker-compose.yml` |
| `error while creating mount source path ... mkdir /run/desktop/mnt/host/e: file exists` | Lỗi Docker Desktop khi dự án nằm ở ổ khác C: | `docker compose down`, `wsl --shutdown`, thoát hẳn rồi mở lại Docker Desktop. Nếu vẫn lỗi, chuyển dự án sang ổ C: |
| Sửa `01_schema.sql` nhưng bảng không thay đổi | Script khởi tạo chỉ chạy khi volume còn rỗng | Khi chưa có dữ liệu thật: `docker compose down -v` rồi `up -d` |
| Đổi `POSTGRES_USER` / `POSTGRES_DB` nhưng không đăng nhập được | User và database chỉ được tạo ở lần khởi tạo đầu | Như trên: `down -v` rồi `up -d` |
| Python trên máy không kết nối được Kafka | Dùng nhầm cổng nội bộ | Từ máy dùng `localhost:9094`; `kafka:9092` chỉ dùng trong Docker |
| `UNKNOWN_TOPIC_OR_PARTITION` | Gõ sai tên topic (đã tắt tự tạo topic) | Kiểm tra tên bằng lệnh `--list` |
| Port đã bị chiếm khi `up` | Cổng trên máy đang được chương trình khác dùng | Đổi `POSTGRES_HOST_PORT` trong `.env` |
| Container tự tắt, log báo thiếu bộ nhớ | RAM cấp cho Docker không đủ | Tăng `memory` trong `.wslconfig` (mục 6.1) |
| Spark lặp lại `Initial job has not accepted any resources` | Một ứng dụng khác (thường là job cũ bị treo) đang giữ hết số nhân của worker | Xem http://localhost:8080: bấm **(kill)** ứng dụng cũ, hoặc `docker compose restart spark-master spark-worker`. Lâu dài: đặt `spark.cores.max` |
| Job đứng yên, không kết thúc | Script có `input()` nhưng chạy không có `-it` | `Ctrl+C`, dừng ứng dụng như dòng trên, chạy lại với `docker exec -it` |
| Job đọc Kafka chạy xong nhưng không có dữ liệu | Sau `docker compose down`, Kafka đã mất hết bản tin (không có volume) | Gửi lại dữ liệu (ví dụ `python producer\kafka_smoke_test.py` cho `sandbox`) rồi chạy lại |
| Lỗi tải connector, báo không ghi được vào `/tmp/.ivy2` | Volume `spark_ivy` được tạo trước khi Dockerfile tạo sẵn thư mục, nên thuộc quyền root | `docker compose down`, `docker volume rm floodprediction-bigdata_spark_ivy`, rồi `build` và `up -d` |
| `psql: warning: extra command-line argument ... ignored`, `unterminated quoted identifier` | PowerShell cắt chuỗi tại dấu `\"` | Gõ truy vấn trong `psql` tương tác (mục 8); đặt tên cột không trùng từ khóa SQL |
| Job streaming không đọc lại dữ liệu cũ dù đặt `startingOffsets = earliest` | Đã có checkpoint, job tiếp tục từ offset đã lưu | Hành vi đúng. Muốn đọc lại thì xóa thư mục checkpoint của job |

---

## 10. Quy ước làm việc nhóm

- **Thay đổi giao diện dữ liệu** (trường bản tin, bảng, topic): cập nhật `CONTRACT.md`, tăng phiên bản, ghi vào lịch sử thay đổi và báo cả nhóm **trước khi** sửa code.
- **Không commit:** `.env`, `venv/`, `data/` (trừ các file `.gitkeep`). Kiểm tra bằng `git status` trước mỗi lần commit.
- **Mỗi thành viên làm trong thư mục của mình** (`streaming/`, `notebooks/`, `ml/`); code dùng chung đặt ở `common/`.
- **Không gửi dữ liệu thử vào `sensor-raw` hay `flood-alerts`**; dùng topic `sandbox`.
- **Thời gian luôn theo UTC**, giá trị thiếu luôn là `null`, seed ngẫu nhiên là `42`.
- **Phiên bản cố định:** không dùng tag `latest` cho image Docker; Python 3.11 cho mọi người.
- **Job Spark luôn chạy trong container** (`docker exec spark-master ... spark-submit`), không chạy PySpark từ Python trên Windows. Đường dẫn trong job là đường dẫn trong container (`/opt/app/...`).
- **Mật khẩu không viết trong code:** job đọc từ biến môi trường (`os.environ`).
- **Mỗi job streaming một thư mục checkpoint riêng**; không xóa checkpoint của job thật nếu không chủ động muốn xử lý lại.
- **Tên cột không trùng từ khóa SQL**; cột lấy từ metadata Kafka dùng tiền tố `kafka_`.
- **Trước khi chạy job mới**, xem http://localhost:8080 để chắc không còn ứng dụng cũ bị treo.