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
7. [Hướng dẫn sử dụng hạ tầng cho thành viên](#7-hướng-dẫn-sử-dụng-hạ-tầng-cho-thành-viên)
8. [Dịch vụ và địa chỉ kết nối](#8-dịch-vụ-và-địa-chỉ-kết-nối)
9. [Lệnh thường dùng](#9-lệnh-thường-dùng)
10. [Xử lý sự cố thường gặp](#10-xử-lý-sự-cố-thường-gặp)
11. [Bảo mật, tài nguyên và sao lưu](#11-bảo-mật-tài-nguyên-và-sao-lưu)
12. [Quy ước làm việc nhóm](#12-quy-ước-làm-việc-nhóm)

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
[PostgreSQL / Parquet]  →  [Dashboard: Grafana, user chỉ đọc]
```

| Thành phần | Vai trò |
|---|---|
| Producer (Python) | Đọc CSV, gắn nhiễu và lỗi cảm biến, gửi bản tin JSON vào Kafka theo nhịp tăng tốc (ví dụ 1 giây = 1 ngày) |
| Kafka | Tầng tiếp nhận: tách rời nguồn và nơi xử lý, làm bộ đệm, giữ thứ tự theo trạm, cho phép phát lại |
| Spark Structured Streaming | Xử lý luồng: kiểm tra schema, Kalman filter, tạo đặc trưng, dự báo, phát cảnh báo |
| PostgreSQL | Lưu kết quả để dashboard truy vấn |
| Parquet (Bronze / Silver / Gold) | Kho dữ liệu dạng file, chia partition theo lưu vực |
| Grafana | Dashboard: bản đồ trạm, mực nước thô/lọc/dự báo, cảnh báo, trạng thái cảm biến. Đọc PostgreSQL bằng user **chỉ đọc** `grafana_reader` |

---

## 4. Cấu trúc thư mục

```
bigdata-floodPrediction/
├── docker-compose.yml        # toàn bộ hạ tầng
├── .env                      # mật khẩu, cổng — KHÔNG đưa lên Git
├── .env.example              # mẫu của .env
├── .gitattributes            # giữ file .sh ở dạng xuống dòng LF
├── CONTRACT.md               # hợp đồng giao diện giữa các thành phần
├── README.md
├── requirements.txt          # thư viện Python của dự án (venv trên máy)
├── data/                     # KHÔNG đưa lên Git
│   ├── raw/                  # CSV gốc, stations_clean.csv
│   ├── noisy/                # dữ liệu nhiễu + ground truth (Parquet)
│   └── lake/                 # bronze / silver / gold (Parquet), _checkpoints/ của Spark Streaming
├── infra/
│   ├── postgres/init/        # script khởi tạo database (chạy khi volume còn rỗng)
│   │   ├── 01_schema.sql         # tạo 5 bảng, khóa, ràng buộc, chỉ mục
│   │   └── 02_grafana_reader.sh  # tạo user chỉ đọc cho Grafana
│   ├── spark/                # Dockerfile + spark-defaults.conf của image Spark
│   └── grafana/provisioning/ # cấu hình Grafana nạp tự động khi khởi động
│       ├── datasources/          # postgres.yaml: kết nối FloodDB
│       └── dashboards/
│           ├── provider.yaml     # nơi Grafana tìm file dashboard
│           └── json/             # các dashboard (.json) xuất từ Grafana
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
| `up -d` và `restart` | `docker compose up -d` **tạo lại** container khi cấu hình hoặc biến môi trường thay đổi (thấy chữ `Recreated`). `docker compose restart` chỉ khởi động lại container cũ, **không nạp biến môi trường mới** |
| Ánh xạ cổng `"HOST:CONTAINER"` | Vế trái là cổng trên máy, vế phải là cổng bên trong container. Container khác luôn dùng cổng bên phải |
| Healthcheck và `depends_on` | Healthcheck cho Docker biết dịch vụ đã **thực sự sẵn sàng** (không chỉ "đã chạy"); `depends_on: condition: service_healthy` bắt dịch vụ khác chờ đến lúc đó |
| Init container | Container chạy một lần để chuẩn bị (ví dụ `kafka-init` tạo topic) rồi thoát với mã `0`. Lệnh viết idempotent (`--if-not-exists`) nên chạy lại nhiều lần vẫn an toàn |
| Named volume và bind mount | **Named volume** (`pg_data`, `grafana_data`) do Docker quản lý, sống lâu hơn container, dùng cho dữ liệu. **Bind mount** (`./infra/...`) trỏ vào thư mục của dự án, dùng cho file cấu hình/script quản lý bằng Git |
| Giới hạn tài nguyên | `deploy.resources.limits.memory` giới hạn RAM từng container, để một dịch vụ không chiếm hết bộ nhớ của các dịch vụ khác |
| Dockerfile, image, container | **Dockerfile** là công thức tạo image (image nền + thư viện cần cài); **image** là thành phẩm đóng gói, bất biến (`docker compose build`); **container** là image đang chạy (`docker compose up`). `docker-compose.yml` chỉ quyết định *chạy những container nào, nối với nhau ra sao*; muốn thay đổi *bên trong* container (cài thêm thư viện) phải dùng Dockerfile. Một image chạy được thành nhiều container (`spark-master`, `spark-worker`) |
| Quyền của named volume | Khi một named volume **rỗng** được gắn vào thư mục đã có trong image, Docker sao chép cả nội dung lẫn quyền truy cập của thư mục đó. Vì vậy thư mục `/tmp/.ivy2` được tạo sẵn (quyền ghi cho user `spark`) trong Dockerfile |
| Ký tự xuống dòng | Windows dùng CRLF, Linux dùng LF. Script `.sh` có CRLF sẽ lỗi trong container (`$'\r': command not found`). `.gitattributes` (`*.sh text eol=lf`) giữ file `.sh` luôn ở dạng LF trên mọi máy |

### 5.2. Kafka

| Khái niệm | Nội dung |
|---|---|
| Vai trò so với database | Kafka là **nhật ký sự kiện có thứ tự**: tiếp nhận tốc độ cao, làm bộ đệm khi lưu lượng tăng đột biến, cho nhiều bên đọc độc lập và cho phép **phát lại**. Database lưu **trạng thái/kết quả** để truy vấn. Hai thành phần bổ sung chứ không thay thế nhau |
| Broker, controller, KRaft | Broker lưu và phân phối bản tin; controller quản lý cụm. Chế độ **KRaft** cho Kafka tự làm controller, không cần ZooKeeper. Cụm của dự án có 1 nút đóng cả hai vai |
| Listener | `INTERNAL` (`kafka:9092`) cho container trong Docker; `EXTERNAL` (`localhost:9094`) cho chương trình trên máy; `CONTROLLER` (9093) dùng nội bộ. Kafka trả về **địa chỉ quảng bá** cho client, nên mỗi phía cần một địa chỉ riêng |
| Topic, partition | Topic chia thành nhiều partition để xử lý song song. Số partition quyết định mức song song tối đa khi đọc |
| Key và thứ tự | Bản tin **cùng key** luôn vào **cùng partition** và giữ thứ tự **trong partition**. Kafka **không** giữ thứ tự giữa các partition. Key = `station_id` để Kalman nhận dữ liệu từng trạm đúng thứ tự |
| Định danh bản tin | Một bản tin được định danh duy nhất bởi (topic, partition, offset); key chỉ là nhãn dùng để chọn partition và có thể lặp lại. Offset là định danh **kỹ thuật**: bản tin gửi lại sẽ có offset mới |
| Producer | `produce()` chỉ đưa bản tin vào hàng đợi và gửi theo lô (batching); `flush()` chờ mọi bản tin được xác nhận; callback `on_delivery` cho biết bản tin đã tới nơi chưa |
| Consumer group, offset, lag | Mỗi nhóm tự ghi nhớ **offset** đã đọc tới; khởi động lại thì đọc tiếp từ đó. **Lag** = số bản tin còn chưa đọc; lag tăng liên tục nghĩa là bên xử lý không theo kịp |
| `__consumer_offsets` | Topic nội bộ (mặc định 50 partition) nơi Kafka lưu offset của các nhóm; tạo tự động khi có nhóm đầu tiên |
| Replication | Cụm 1 broker nên replication = 1, không có dự phòng. Cụm thật thường dùng 3 broker, replication 3 |
| Schema | Kafka không kiểm tra nội dung bản tin (chỉ là byte). Schema được thực thi ở hai đầu: Producer kiểm tra trước khi gửi, Spark tách bản tin lỗi vào vùng cách ly |

### 5.3. PostgreSQL

| Khái niệm | Nội dung |
|---|---|
| Script khởi tạo | File `.sql` và `.sh` trong `/docker-entrypoint-initdb.d` tự chạy theo thứ tự tên **chỉ khi database được tạo lần đầu** (volume rỗng). User, mật khẩu, tên database cũng chỉ được tạo lúc đó: đổi `.env` sau này **không** đổi mật khẩu trong database |
| `.sql` và `.sh` | File `.sql` không đọc được biến môi trường; cần mật khẩu từ `.env` thì dùng `.sh` gọi `psql` |
| Kiểu dữ liệu | `TIMESTAMPTZ`: thời điểm tuyệt đối theo UTC. `DOUBLE PRECISION`: số đo gần đúng, nhanh và gọn hơn `NUMERIC` |
| Khóa chính ghép | (`station_id`, `event_time`) là khóa **tự nhiên (nghiệp vụ)**: một trạm, một thời điểm chỉ có một số đo, dù bản tin được gửi hay xử lý lại bao nhiêu lần |
| Khóa ngoại | Chỉ chấp nhận trạm đã có trong `stations`: phải nạp `stations` trước, xóa bảng con trước bảng cha |
| `CHECK` | Chặn giá trị vô lý ngay tại database (mưa âm, xác suất ngoài [0, 1]…) |
| Chỉ mục | Tăng tốc truy vấn nhưng làm chậm việc ghi; chỉ tạo khi có truy vấn thật cần. **Partial index** chỉ chứa một phần dòng (ví dụ các dòng có lỗi) nên rất nhỏ |
| Ghi idempotent | Spark Streaming đảm bảo "ít nhất một lần" nên có thể xử lý lại một lô. Ghi bằng `INSERT ... ON CONFLICT DO NOTHING` để ghi lại bao nhiêu lần kết quả vẫn như một lần |
| Role và quyền | Mỗi thành phần dùng user với **quyền tối thiểu** cần thiết: `floodprediction` (chủ sở hữu, pipeline ghi dữ liệu), `grafana_reader` (chỉ `SELECT`). `ALTER DEFAULT PRIVILEGES` để quyền đọc tự áp dụng cho bảng tạo sau này |
| Xác thực (`pg_hba.conf`) | Kết nối **từ bên trong** container (`docker exec ... psql`) được tin cậy, **không hỏi mật khẩu**. Kết nối **qua mạng** (Spark, Grafana, DBeaver, `psql -h postgres`) bắt buộc đúng mật khẩu. Vì vậy muốn kiểm tra mật khẩu phải thử qua mạng |

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
| Cấp phát tài nguyên | Ở chế độ standalone, mỗi ứng dụng mặc định **chiếm hết số nhân còn trống** (2 nhân) và giữ đến khi kết thúc; ứng dụng sau phải chờ. Dự án chạy **một ứng dụng tại một thời điểm**; cần song song thì ghi đè `--conf spark.cores.max=1` |
| Structured Streaming | `readStream` đọc liên tục; dữ liệu mới được gom thành **micro-batch** theo `trigger` (ví dụ 5 giây). `foreachBatch` giao mỗi micro-batch cho một hàm như DataFrame thường, nên dùng được mọi lệnh ghi batch |
| Checkpoint | Thư mục lưu offset đã xử lý của job streaming. Khởi động lại thì đọc tiếp, không lặp; `startingOffsets` chỉ có tác dụng ở lần chạy đầu tiên (khi chưa có checkpoint). Spark không lưu offset vào consumer group của Kafka |
| Exactly-once | Checkpoint **cộng với** ghi idempotent (khóa chính + `ON CONFLICT DO NOTHING`) mới bảo đảm mỗi bản tin được ghi đúng một lần, kể cả khi xử lý lại |
| Báo cáo tiến độ streaming | Mỗi micro-batch có `numInputRows`, `inputRowsPerSecond`, `processedRowsPerSecond`, `durationMs` (`addBatch`, `commitOffsets`…): nguồn số liệu thông lượng và độ trễ. Xử lý nhanh hơn tốc độ dữ liệu đến thì hệ thống theo kịp |

### 5.6. Grafana

| Khái niệm | Nội dung |
|---|---|
| Vai trò | Công cụ **trực quan hóa**: không lưu dữ liệu của dự án, mà **truy vấn trực tiếp** PostgreSQL mỗi lần vẽ biểu đồ |
| Datasource | Một kết nối tới nguồn dữ liệu (địa chỉ, user, mật khẩu). Dự án có một datasource `FloodDB` (uid `flood-postgres`), đăng nhập bằng `grafana_reader` |
| `access: proxy` | Máy chủ Grafana (trong Docker) truy vấn database, không phải trình duyệt. Vì vậy địa chỉ là `postgres:5432` (địa chỉ trong Docker) |
| Dashboard và panel | Dashboard là một trang gồm nhiều panel, dùng chung **khoảng thời gian** (góc trên phải). Mỗi panel = **một câu SQL** + **một kiểu hiển thị** (Time series, Table, Stat, Geomap…) |
| Cột `time` | Panel dạng chuỗi thời gian cần một cột tên `time` (`event_time AS "time"`), dữ liệu sắp xếp theo thời gian. Các cột số còn lại thành các đường trên biểu đồ |
| `$__timeFilter(cột)` | Macro Grafana tự thay bằng điều kiện lọc theo khoảng thời gian đang chọn; đổi khoảng thời gian thì mọi panel tự truy vấn lại |
| Provisioning | Datasource và dashboard được khai báo bằng **file** trong `infra/grafana/provisioning/`, Grafana đọc mỗi khi khởi động. Nhờ đó cấu hình nằm trong Git và mọi người dựng ra giống hệt nhau. Biến `${...}` trong file được thay bằng biến môi trường của container |
| Database nội bộ của Grafana | Grafana lưu user, cài đặt, dashboard tạo trên giao diện trong SQLite ở volume `grafana_data`. Tài khoản admin (`GF_SECURITY_ADMIN_*`) **chỉ được đặt ở lần khởi động đầu** của volume này |
| `editable: false` | Datasource provisioning không sửa được trên giao diện; muốn đổi thì sửa file, để file luôn là **nguồn sự thật duy nhất** |
| Quyền tối thiểu | Dashboard chỉ cần đọc, nên Grafana dùng `grafana_reader` (chỉ `SELECT`). Lỡ viết câu lệnh sửa/xóa trong panel, hay thông tin đăng nhập của Grafana bị lộ, dữ liệu vẫn an toàn |

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
| Python | **3.11** (bộ cài Windows cuối cùng: 3.11.9; PySpark 3.5 chưa hỗ trợ bản mới hơn) | `py --list` |

Nếu RAM cấp cho Docker thấp hơn 6 GB, tạo file `%USERPROFILE%\.wslconfig`:

```ini
[wsl2]
memory=8GB
```

rồi chạy `wsl --shutdown` và mở lại Docker Desktop.

Nên đặt dự án trên **ổ C:**: Docker Desktop đôi khi không gắn được thư mục ở ổ khác (xem mục 10).

### 6.2. Chuẩn bị dự án (làm một lần)

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

Mở `.env` và điền:

| Biến | Giá trị |
|---|---|
| `KAFKA_CLUSTER_ID` | Chuỗi vừa sinh ở lệnh trên |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | Tài khoản chủ của database |
| `POSTGRES_HOST_PORT` | Cổng PostgreSQL mở ra máy; đổi nếu cổng mặc định đã bị chiếm (`netstat -ano \| findstr :5433`) |
| `GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD` | Tài khoản đăng nhập giao diện Grafana |
| `GRAFANA_DB_PASSWORD` | Mật khẩu của user chỉ đọc `grafana_reader` |

Quy tắc cho mật khẩu và `.env`: **chỉ dùng chữ và số** (không `$`, `'`, `"`, `#`, khoảng trắng), không đặt trong ngoặc kép, không có khoảng trắng quanh dấu `=`. Ghi chú đặt trên dòng riêng, bắt đầu bằng `#`.

**3. Tạo môi trường Python**

```powershell
py -3.11 -m venv venv
.\venv\Scripts\Activate.ps1
python --version            # phải là 3.11.x
pip install -r requirements.txt
```

Nếu PowerShell chặn chạy script: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`. Trong VS Code: `Ctrl+Shift+P` → **Python: Select Interpreter** → chọn `venv\Scripts\python.exe`.

### 6.3. Hạ tầng được xây dựng

Toàn bộ hạ tầng chạy trong Docker, mô tả trong `docker-compose.yml`:

| Container | Image | Bản chất | Lưu trữ |
|---|---|---|---|
| `kafka` | `apache/kafka:3.9.0` | Cụm Kafka **1 nút**, chế độ KRaft (vừa broker vừa controller); 3 listener; tắt tự tạo topic; heap giới hạn 512 MB | Không có volume: dữ liệu mất khi xóa container (có thể phát lại từ CSV) |
| `kafka-init` | `apache/kafka:3.9.0` | Init container: chờ Kafka healthy, tạo 3 topic rồi tự thoát | — |
| `kafka-ui` | `provectuslabs/kafka-ui:v0.7.2` | Giao diện web quản trị Kafka; RAM giới hạn 512 MB | — |
| `postgres` | `postgres:16` | Database kết quả, múi giờ UTC; ở lần khởi tạo đầu tự chạy `01_schema.sql` (5 bảng) và `02_grafana_reader.sh` (user chỉ đọc); RAM giới hạn 512 MB | Named volume `pg_data`: dữ liệu giữ lại khi tắt hệ thống |
| `spark-master` | `floodprediction/spark:3.5.3` (build từ `infra/spark/Dockerfile`) | Master của cụm Spark standalone; đồng thời là nơi chạy **driver** khi `spark-submit`; nhận biến `POSTGRES_*`; RAM giới hạn 1,5 GB | Bind mount `streaming/`, `common/`, `data/`; named volume `spark_ivy` (lưu đệm connector) |
| `spark-worker` | `floodprediction/spark:3.5.3` | Worker đăng ký với `spark://spark-master:7077`, cung cấp **2 nhân, 2 GB** cho executor; RAM container giới hạn 2,5 GB | Bind mount `streaming/`, `common/`, `data/` (cùng đường dẫn với master) |
| `grafana` | `grafana/grafana:11.3.0` | Dashboard; chờ PostgreSQL healthy; nạp datasource `FloodDB` và dashboard từ `infra/grafana/provisioning/`; RAM giới hạn 256 MB | Named volume `grafana_data` (database nội bộ của Grafana); bind mount thư mục provisioning (chỉ đọc) |

**Topic Kafka do `kafka-init` tạo**

| Topic | Key | Partition | Dùng cho |
|---|---|---|---|
| `sensor-raw` | `station_id` | 5 | Bản tin cảm biến. **Không gửi dữ liệu thử vào đây** |
| `flood-alerts` | `station_id` | 1 | Cảnh báo ngập |
| `sandbox` | tùy ý | 5 | Thử nghiệm |

**Bảng và user PostgreSQL**

- Bảng: `stations`, `sensor_readings`, `predictions`, `flood_alerts`, `pipeline_metrics`. Chi tiết cột, khóa, ràng buộc và chỉ mục: [CONTRACT.md](CONTRACT.md) mục 8.
- User: `floodprediction` (chủ sở hữu, theo `.env`) và `grafana_reader` (chỉ đọc).

**Image Spark riêng** (`infra/spark/Dockerfile`): lấy image chính thức làm nền, cài thêm `numpy`, `pandas`, `pyarrow` (bắt buộc cho `applyInPandas` của Kalman), `psycopg2-binary` (ghi `ON CONFLICT` vào PostgreSQL), `pyyaml`; chép `spark-defaults.conf` vào image. Chi tiết phiên bản và cấu hình mặc định: [CONTRACT.md](CONTRACT.md) mục 2.1, 2.4.

### 6.4. Khởi động hạ tầng lần đầu

**Bước 1: kiểm tra cấu hình** (không khởi động gì, chỉ đọc file và thay biến từ `.env`):

```powershell
docker compose config
```

Lệnh này in cả mật khẩu ra màn hình: **không dán kết quả lên nhóm chat hay vào báo cáo**.

**Bước 2: build image Spark và khởi động**

```powershell
docker compose build
docker compose up -d
```

`build` chỉ cần chạy lần đầu và mỗi khi sửa `infra/spark/Dockerfile` hoặc `spark-defaults.conf`; lần đầu mất vài phút.

Thứ tự Docker Compose thực hiện:

1. Tạo mạng nội bộ `floodprediction-bigdata_default` và các volume `pg_data`, `spark_ivy`, `grafana_data` (nếu chưa có).
2. Khởi động `kafka` và `postgres`; mỗi dịch vụ tự kiểm tra sức khỏe (healthcheck).
3. Nếu volume `pg_data` còn rỗng: `postgres` chạy `01_schema.sql` rồi `02_grafana_reader.sh`.
4. Khi `kafka` **healthy**: khởi động `kafka-ui` và chạy `kafka-init` để tạo topic.
5. Khi `postgres` **healthy**: khởi động `grafana`, nạp datasource và dashboard từ thư mục provisioning.
6. Khởi động `spark-master`, rồi `spark-worker` đăng ký với master.

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
| `grafana` | `Up` |

**Bước 4: kiểm tra từng thành phần**

Kafka:

```powershell
# Danh sách topic phải có flood-alerts, sandbox, sensor-raw
docker compose logs kafka-init

# Gửi/nhận thử từ máy (cổng EXTERNAL, topic sandbox)
python producer\kafka_smoke_test.py
```

Kafka UI: http://localhost:8085.

PostgreSQL:

```powershell
# Phải có 5 bảng
docker exec -it postgres psql -U <POSTGRES_USER> -d <POSTGRES_DB> -c "\dt"

# User grafana_reader đăng nhập được QUA MẠNG bằng mật khẩu trong .env (phải in ra 1 dòng, không báo lỗi xác thực)
docker exec postgres sh -c 'PGPASSWORD=$GRAFANA_DB_PASSWORD psql -h postgres -U grafana_reader -d $POSTGRES_DB -c "SELECT 1;"'
```

Spark:

```powershell
# Phiên bản và thư viện bên trong image
docker exec spark-master /opt/spark/bin/spark-submit --version
docker exec spark-master python3 -c "import numpy, pandas, pyarrow, psycopg2, yaml; print('ok')"

# Chạy job thử trên cụm
docker exec spark-master /opt/spark/bin/spark-submit /opt/app/streaming/spark_smoke_test.py
```

Spark UI: http://localhost:8080, phải có **Alive Workers: 1**, *Cores 2, Memory 2.0 GiB*. Lần đầu chạy job có đọc Kafka hoặc ghi PostgreSQL, Spark tải connector từ Internet (khoảng một phút); các lần sau dùng bộ nhớ đệm.

Grafana:

1. Mở http://localhost:3000, đăng nhập bằng `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD`.
2. **☰ → Connections → Data sources → FloodDB** → kéo xuống cuối → **Test**: phải hiện **"Database Connection OK"**.
3. **☰ → Dashboards**: thư mục **Flood Prediction** chứa các dashboard nạp từ `provisioning/dashboards/json/`.

### 6.5. Khởi động và tắt hằng ngày

| Việc | Lệnh | Ghi chú |
|---|---|---|
| Bật hệ thống | `docker compose up -d` | Không cần `build` nếu image Spark không đổi |
| Xem trạng thái | `docker compose ps -a` | |
| Tắt hệ thống | `docker compose down` | Dữ liệu Kafka mất (kể cả offset consumer group); PostgreSQL, Grafana, bộ đệm connector giữ lại |
| Sau khi bật lại | Gửi lại dữ liệu vào Kafka trước khi thử các job đọc Kafka | `kafka-init` tạo lại topic **rỗng** |

### 6.6. Áp dụng thay đổi cấu hình

| Bạn vừa sửa | Lệnh cần chạy |
|---|---|
| `.env` hoặc `environment` trong `docker-compose.yml` | `docker compose up -d` (phải thấy `Recreated`; `restart` **không** nạp biến mới) |
| `infra/spark/Dockerfile`, `spark-defaults.conf` | `docker compose build` rồi `docker compose up -d` |
| File trong `infra/grafana/provisioning/` | `docker compose restart grafana` |
| `infra/postgres/init/*` trên database **đã có** | Chạy tay: `docker exec postgres bash /docker-entrypoint-initdb.d/<file>.sh` (hoặc `psql -f` cho file `.sql`). Script chỉ tự chạy khi volume rỗng |
| `POSTGRES_PASSWORD` trên database đã có | Đồng bộ trong `psql`: `ALTER ROLE <POSTGRES_USER> WITH PASSWORD '...';` |
| `GRAFANA_DB_PASSWORD` | `docker compose up -d`, chạy lại `02_grafana_reader.sh` (script tự cập nhật mật khẩu), rồi `docker compose restart grafana` |

### 6.7. Làm lại từ đầu (mức độ xóa)

| Muốn làm lại | Lệnh | Mất gì |
|---|---|---|
| Chỉ Kafka | `docker compose down` rồi `up -d` | Bản tin và offset Kafka |
| Chỉ Grafana | `docker compose rm -sf grafana`, `docker volume rm floodprediction-bigdata_grafana_data`, `docker compose up -d grafana` | Dashboard tạo trên giao diện chưa xuất JSON, tài khoản Grafana |
| Chỉ bộ đệm connector Spark | `docker compose down`, `docker volume rm floodprediction-bigdata_spark_ivy`, `docker compose up -d` | Phải tải lại connector |
| **Toàn bộ** | `docker compose down -v` rồi `up -d` | **Mọi dữ liệu PostgreSQL**, Grafana, bộ đệm. Chỉ dùng khi chưa có dữ liệu cần giữ |

---

## 7. Hướng dẫn sử dụng hạ tầng cho thành viên

### 7.1. Cho mọi thành viên

- Bật/tắt hệ thống theo mục 6.5. Trước khi dùng, kiểm tra `docker compose ps -a`.
- **Code chạy trên máy** (Producer, notebook, script thử): dùng venv Python 3.11, kết nối bằng địa chỉ "từ máy" ở mục 8 (`localhost:9094`, `localhost:<POSTGRES_HOST_PORT>`).
- **Job Spark**: luôn chạy trong container (`docker exec spark-master ... spark-submit`), đường dẫn bên trong container (`/opt/app/...`), kết nối bằng địa chỉ "trong Docker" (`kafka:9092`, `postgres:5432`).
- **Xem dữ liệu**:
  - Kafka: Kafka UI http://localhost:8085.
  - PostgreSQL: `psql` (mục 9) hoặc công cụ giao diện như **DBeaver** (Host `localhost`, Port `POSTGRES_HOST_PORT`, Database/User/Password theo `.env`). DBeaver vẽ được sơ đồ quan hệ giữa các bảng (chuột phải `public` → *View Diagram*).
  - Spark: http://localhost:8080 (cụm), http://localhost:4040 (job đang chạy).
- **Thử nghiệm Kafka**: chỉ dùng topic `sandbox`.
- Trước khi chạy job Spark mới, xem http://localhost:8080: cụm chỉ chạy **một ứng dụng tại một thời điểm**.

### 7.2. Cho người làm dashboard (TV2)

**Quy trình làm dashboard:**

1. Đăng nhập Grafana, **☰ → Dashboards → New → New dashboard → Add visualization**, chọn datasource **FloodDB**.
2. Ở khung truy vấn, chuyển nút **Builder / Code** sang **Code**, viết SQL, bấm **Run query**.
3. Chọn kiểu hiển thị ở cột phải (Time series, Table, Stat, Geomap…), đặt **Title**.
4. **Save dashboard**.
5. **Xuất ra file để lưu vào Git:** mở dashboard → **Share / Export → Export as JSON → Download**. Lưu file vào `infra/grafana/provisioning/dashboards/json/<tên>.json`, rồi commit.
6. Kiểm tra: `docker compose restart grafana`, dashboard xuất hiện trong thư mục **Flood Prediction**.

Dashboard chỉ lưu trên giao diện sẽ **mất** khi xóa volume `grafana_data` và **không có** trên máy người khác. Luôn xuất JSON.

**Mẫu câu SQL cho panel chuỗi thời gian:**

```sql
SELECT event_time AS "time",
       level_raw      AS "Mực nước đo",
       level_filtered AS "Mực nước đã lọc"
FROM sensor_readings
WHERE station_id = 'ASIA_0009'
  AND $__timeFilter(event_time)
ORDER BY event_time;
```

**Lưu ý:**

- Dữ liệu thật có `event_time` từ **2000 đến 2024**. Khoảng thời gian mặc định "Last 6 hours / Last 30 days" sẽ **không thấy gì**: chọn khoảng tuyệt đối (ví dụ 2022-01-01 → 2024-12-31).
- `FloodDB` dùng user **chỉ đọc**: panel chỉ dùng `SELECT`.
- Dùng **biến dashboard** (Dashboard settings → Variables, kiểu *Query*: `SELECT station_id FROM stations ORDER BY 1`) để chọn trạm thay vì viết cứng mã trạm trong SQL.

### 7.3. Cho người làm mô hình (TV3)

- Huấn luyện mô hình bằng PySpark **chế độ local** trên máy (venv 3.11) hoặc pandas/scikit-learn; không gửi job huấn luyện vào cụm khi job streaming đang chạy.
- Dự báo và cảnh báo trong pipeline được ghi vào `predictions`, `flood_alerts` và topic `flood-alerts` theo [CONTRACT.md](CONTRACT.md) mục 4.1, 8; ghi bằng `ON CONFLICT`.

---

## 8. Dịch vụ và địa chỉ kết nối

| Dịch vụ | Image | Từ trong Docker | Từ máy (Windows) |
|---|---|---|---|
| Kafka (KRaft, 1 broker) | `apache/kafka:3.9.0` | `kafka:9092` | `localhost:9094` |
| Kafka UI | `provectuslabs/kafka-ui:v0.7.2` | — | http://localhost:8085 |
| PostgreSQL | `postgres:16` | `postgres:5432` | `localhost:${POSTGRES_HOST_PORT}` (mặc định 5433) |
| Spark master | `floodprediction/spark:3.5.3` | `spark://spark-master:7077` | http://localhost:8080 |
| Spark worker | `floodprediction/spark:3.5.3` | — | http://localhost:8081 |
| Ứng dụng Spark đang chạy | — | — | http://localhost:4040 (chỉ khi có job) |
| Grafana | `grafana/grafana:11.3.0` | `grafana:3000` | http://localhost:3000 |

**Tài khoản**

| Tài khoản | Dùng cho | Quyền |
|---|---|---|
| `POSTGRES_USER` (`floodprediction`) | Pipeline Spark, quản trị database | Chủ sở hữu |
| `grafana_reader` | Datasource `FloodDB` của Grafana | Chỉ `SELECT` |
| `GRAFANA_ADMIN_USER` | Đăng nhập giao diện Grafana | Quản trị Grafana |

---

## 9. Lệnh thường dùng

### Docker Compose

| Lệnh | Tác dụng |
|---|---|
| `docker compose up -d` | Khởi động; tạo lại container khi cấu hình/biến môi trường thay đổi |
| `docker compose ps -a` | Xem trạng thái, kể cả container đã thoát |
| `docker compose logs -f <dịch-vụ>` | Xem log trực tiếp (`Ctrl+C` để thoát) |
| `docker compose logs <dịch-vụ> \| Select-String -Pattern "error"` | Lọc dòng log có chữ "error" (PowerShell) |
| `docker compose restart <dịch-vụ>` | Khởi động lại container cũ (không nạp biến môi trường mới) |
| `docker compose down` | Tắt hệ thống. **Dữ liệu Kafka mất**, dữ liệu PostgreSQL, Grafana **giữ lại** |
| `docker compose down -v` | Tắt và **xóa cả volume**: mất toàn bộ dữ liệu PostgreSQL, Grafana. Chỉ dùng khi muốn làm lại từ đầu |
| `docker exec <container> sh -c 'echo ${#BIEN}'` | In **độ dài** một biến môi trường trong container (kiểm tra đã nhận mật khẩu mà không in mật khẩu ra) |

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
# Vào psql bằng user chủ (từ bên trong container: không hỏi mật khẩu)
docker exec -it postgres psql -U <POSTGRES_USER> -d <POSTGRES_DB>

# Vào psql bằng grafana_reader qua mạng (có kiểm tra mật khẩu)
docker exec -it postgres sh -c 'PGPASSWORD=$GRAFANA_DB_PASSWORD psql -h postgres -U grafana_reader -d $POSTGRES_DB'

# Chạy lại script tạo/cập nhật user chỉ đọc
docker exec postgres bash /docker-entrypoint-initdb.d/02_grafana_reader.sh
```

Trong `psql`:

| Lệnh | Xem gì |
|---|---|
| `\l` | Danh sách database |
| `\conninfo` | Đang kết nối database nào, bằng user nào, qua socket hay mạng |
| `\dt`, `\dt+` | Danh sách bảng (kèm dung lượng) |
| `\d <bảng>` | Cấu trúc bảng: cột, kiểu, khóa, chỉ mục, ràng buộc |
| `\di` | Danh sách chỉ mục |
| `\du` | Danh sách user (role) |
| `\q` | Thoát |

Khi kết quả dài hiện `(END)`, nhấn `q` để thoát màn hình phân trang. Dấu nhắc `=#` là user chủ sở hữu, `=>` là user thường.

Đếm số dòng của mọi bảng (dùng để đối soát):

```sql
SELECT 'stations' AS bang, COUNT(*) FROM stations
UNION ALL SELECT 'sensor_readings', COUNT(*) FROM sensor_readings
UNION ALL SELECT 'predictions', COUNT(*) FROM predictions
UNION ALL SELECT 'flood_alerts', COUNT(*) FROM flood_alerts
UNION ALL SELECT 'pipeline_metrics', COUNT(*) FROM pipeline_metrics;
```

Truy vấn có dấu ngoặc kép (ví dụ tên cột trùng từ khóa SQL) nên gõ **trong `psql` tương tác** thay vì `psql -c "..."`: PowerShell không dùng `\` làm ký tự thoát và có thể bỏ mất dấu `"` lồng bên trong, nên câu lệnh dễ bị cắt.

### Spark

```powershell
# Chạy một job (đường dẫn bên trong container)
docker exec spark-master /opt/spark/bin/spark-submit /opt/app/streaming/<file>.py

# Job có input() hoặc job streaming cần dừng bằng Ctrl+C
docker exec -it spark-master /opt/spark/bin/spark-submit /opt/app/streaming/<file>.py

# Ghi đè cấu hình cho một lần chạy
docker exec -it spark-master /opt/spark/bin/spark-submit --conf spark.cores.max=1 /opt/app/streaming/<file>.py

# Khởi động lại cụm Spark (giải phóng ứng dụng bị treo), không ảnh hưởng Kafka, PostgreSQL
docker compose restart spark-master spark-worker

# Log của master/worker
docker compose logs -f spark-worker
```

Job streaming: xóa thư mục `data/lake/_checkpoints/<tên-job>/` khi muốn đọc lại topic từ đầu (dữ liệu sẽ bị ghi lại nếu đích không idempotent).

### Grafana

```powershell
# Nạp lại provisioning (datasource, dashboard) sau khi sửa file
docker compose restart grafana

# Xem log liên quan provisioning và lỗi
docker compose logs grafana | Select-String -Pattern "provisioning|datasource|error"
```

---

## 10. Xử lý sự cố thường gặp

### Docker và môi trường

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `failed to read .env: unexpected character ... in variable name` | Đặt nội dung YAML (như `ports:`) vào `.env` | `.env` chỉ chứa `TÊN=GIÁ_TRỊ`; cấu hình dịch vụ đặt trong `docker-compose.yml` |
| `error while creating mount source path ... mkdir /run/desktop/mnt/host/e: file exists` | Lỗi Docker Desktop khi dự án nằm ở ổ khác C: | `docker compose down`, `wsl --shutdown`, thoát hẳn rồi mở lại Docker Desktop. Nếu vẫn lỗi, chuyển dự án sang ổ C: |
| Sửa `.env` nhưng container vẫn dùng giá trị cũ | Dùng `restart` thay vì `up -d` | `docker compose up -d` (phải thấy `Recreated`) |
| Port đã bị chiếm khi `up` | Cổng trên máy đang được chương trình khác dùng | Đổi `POSTGRES_HOST_PORT` trong `.env` |
| Container tự tắt, log báo thiếu bộ nhớ | RAM cấp cho Docker không đủ | Tăng `memory` trong `.wslconfig` (mục 6.1) |
| Script `.sh` báo `$'\r': command not found` hoặc `bad interpreter` | File `.sh` có ký tự xuống dòng CRLF của Windows | Trong VS Code bấm **CRLF** ở thanh trạng thái → **LF**, lưu lại. Kiểm tra `.gitattributes` có `*.sh text eol=lf` |
| VS Code vẫn hiện Python 3.14 | Chưa chọn interpreter của venv | `Ctrl+Shift+P` → *Python: Select Interpreter* → `venv\Scripts\python.exe`, mở terminal mới |

### Kafka

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| Python trên máy không kết nối được Kafka | Dùng nhầm cổng nội bộ | Từ máy dùng `localhost:9094`; `kafka:9092` chỉ dùng trong Docker |
| `UNKNOWN_TOPIC_OR_PARTITION` | Gõ sai tên topic (đã tắt tự tạo topic) | Kiểm tra tên bằng lệnh `--list` |
| Topic rỗng sau khi bật lại hệ thống | Kafka không có volume, `down` xóa hết bản tin | Gửi lại dữ liệu (ví dụ `python producer\kafka_smoke_test.py` cho `sandbox`) |

### PostgreSQL

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| Sửa `01_schema.sql` nhưng bảng không thay đổi | Script khởi tạo chỉ chạy khi volume còn rỗng | Khi chưa có dữ liệu thật: `docker compose down -v` rồi `up -d`; khi đã có dữ liệu: chạy tay bằng `psql` |
| Đổi `POSTGRES_USER` / `POSTGRES_DB` / `POSTGRES_PASSWORD` nhưng không đăng nhập được qua mạng | User, database, mật khẩu chỉ được đặt ở lần khởi tạo đầu | Đổi mật khẩu: `ALTER ROLE ... WITH PASSWORD '...';`. Đổi user/database: `down -v` rồi `up -d` (khi chưa có dữ liệu thật) |
| `docker exec ... psql` không hỏi mật khẩu dù đã đặt | Kết nối từ bên trong container được tin cậy (`trust`) | Hành vi đúng. Kiểm tra mật khẩu bằng kết nối qua mạng (`psql -h postgres`) |
| `psql: warning: extra command-line argument ... ignored`, `unterminated quoted identifier` | PowerShell cắt chuỗi tại dấu `\"` | Gõ truy vấn trong `psql` tương tác (mục 9); đặt tên cột không trùng từ khóa SQL |
| `permission denied for table ...` khi dùng `grafana_reader` | User chỉ có quyền đọc | Hành vi đúng. Ghi dữ liệu bằng user chủ |

### Spark

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| Spark lặp lại `Initial job has not accepted any resources` | Một ứng dụng khác (thường là job cũ bị treo) đang giữ hết số nhân của worker | Xem http://localhost:8080: bấm **(kill)** ứng dụng cũ, hoặc `docker compose restart spark-master spark-worker` |
| Job đứng yên, không kết thúc | Script có `input()` nhưng chạy không có `-it` | `Ctrl+C`, dừng ứng dụng như dòng trên, chạy lại với `docker exec -it` |
| Job đọc Kafka chạy xong nhưng không có dữ liệu | Sau `docker compose down`, Kafka đã mất hết bản tin | Gửi lại dữ liệu rồi chạy lại |
| Lỗi tải connector, báo không ghi được vào `/tmp/.ivy2` | Volume `spark_ivy` được tạo trước khi Dockerfile tạo sẵn thư mục, nên thuộc quyền root | `docker compose down`, `docker volume rm floodprediction-bigdata_spark_ivy`, rồi `build` và `up -d` |
| Job streaming không đọc lại dữ liệu cũ dù đặt `startingOffsets = earliest` | Đã có checkpoint, job tiếp tục từ offset đã lưu | Hành vi đúng. Muốn đọc lại thì xóa thư mục checkpoint của job |

### Grafana

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `pq: password authentication failed for user "grafana_reader"` | Mật khẩu của `grafana_reader` trong database khác `GRAFANA_DB_PASSWORD` mà Grafana dùng | Kiểm tra theo 3 tầng: (1) `docker exec postgres sh -c 'echo ${#GRAFANA_DB_PASSWORD}'` và tương tự với `grafana` phải ra cùng một số khác 0; (2) chạy lại `02_grafana_reader.sh`, thử đăng nhập qua mạng (mục 6.4); (3) `docker compose restart grafana` rồi **Test** lại |
| Script báo `NOTICE: empty string is not a valid password` | Container `postgres` chưa nhận biến `GRAFANA_DB_PASSWORD` | Thêm biến vào `environment` của `postgres`, `docker compose up -d`, chạy lại script |
| Script báo `role "grafana_reader" already exists` | Phiên bản script cũ không idempotent | Dùng bản script có khối `IF NOT EXISTS ... ELSE ALTER ROLE` |
| Không thấy datasource `FloodDB` | File provisioning không nằm trong thư mục được gắn, hoặc sai tên thư mục | Đúng cấu trúc: `infra/grafana/provisioning/datasources/` và `.../dashboards/` (có chữ `s`). Rồi `docker compose restart grafana` |
| Không đăng nhập được Grafana sau khi đổi mật khẩu admin trong `.env` | Tài khoản admin chỉ được đặt ở lần khởi động đầu của volume `grafana_data` | Làm lại Grafana (mục 6.7) |
| Panel trống, "No data" | Khoảng thời gian dashboard không chứa dữ liệu, hoặc thiếu cột `time` | Chọn khoảng thời gian tuyệt đối 2000–2024; đặt `event_time AS "time"`; bấm **Query inspector** để xem SQL thực tế |
| Không sửa được datasource trên giao diện | Datasource provisioning có `editable: false` | Hành vi đúng. Sửa file `datasources/postgres.yaml` |
| Dashboard biến mất sau khi làm lại Grafana | Dashboard chỉ lưu trên giao diện, chưa xuất JSON | Luôn xuất JSON vào `provisioning/dashboards/json/` (mục 7.2) |

---

## 11. Bảo mật, tài nguyên và sao lưu

### 11.1. Bảo mật

- **Không commit `.env`.** Kiểm tra: `git ls-files .env` phải không in ra gì. Nếu lỡ commit: `git rm --cached .env`, commit, và **đổi mọi mật khẩu** (mật khẩu cũ vẫn còn trong lịch sử Git).
- `.env.example` chỉ chứa giá trị mẫu (`change_me`), không chứa mật khẩu thật.
- Mỗi thành phần dùng user có quyền tối thiểu (mục 8, *Tài khoản*).
- Hạn chế của môi trường phát triển (ghi vào báo cáo): kết nối Kafka và PostgreSQL không mã hóa (`PLAINTEXT`, `sslmode: disable`); Kafka 1 broker, không có bản sao dự phòng.

### 11.2. Tài nguyên

Giới hạn RAM theo container (tổng khoảng 6 GB trên 7,6 GB cấp cho Docker):

| Dịch vụ | Giới hạn |
|---|---|
| Kafka | heap 512 MB (~0,7 GB tổng) |
| Kafka UI | 512 MB |
| PostgreSQL | 512 MB |
| Spark master + driver | 1,5 GB |
| Spark worker + executor | 2,5 GB |
| Grafana | 256 MB |

Xem dung lượng đĩa Docker đang dùng: `docker system df -v`. Mọi image, container, volume nằm trong file đĩa ảo của WSL 2 trên **ổ C:**.

### 11.3. Sao lưu và khôi phục PostgreSQL

```powershell
# Sao lưu toàn bộ cấu trúc + dữ liệu ra file
docker exec postgres pg_dump -U <POSTGRES_USER> -d <POSTGRES_DB> > backup.sql

# Khôi phục vào database (đang trống)
Get-Content backup.sql | docker exec -i postgres psql -U <POSTGRES_USER> -d <POSTGRES_DB>
```

Sao lưu trước khi chạy `docker compose down -v` hoặc trước các thay đổi lớn. Không commit file sao lưu.

---

## 12. Quy ước làm việc nhóm

- **Thay đổi giao diện dữ liệu** (trường bản tin, bảng, topic): cập nhật `CONTRACT.md`, tăng phiên bản, ghi vào lịch sử thay đổi và báo cả nhóm **trước khi** sửa code.
- **Không commit:** `.env`, `venv/`, `data/` (trừ các file `.gitkeep`), file sao lưu. Kiểm tra bằng `git status` trước mỗi lần commit.
- **Mỗi bước/tính năng một nhánh Git** tạo từ `main` mới nhất; merge vào `main` qua Pull Request.
- **Mỗi thành viên làm trong thư mục của mình** (`streaming/`, `notebooks/`, `ml/`); code dùng chung đặt ở `common/`.
- **Không gửi dữ liệu thử vào `sensor-raw` hay `flood-alerts`**; dùng topic `sandbox`.
- **Thời gian luôn theo UTC**, giá trị thiếu luôn là `null`, seed ngẫu nhiên là `42`.
- **Phiên bản cố định:** không dùng tag `latest` cho image Docker; Python 3.11 cho mọi người.
- **Job Spark luôn chạy trong container** (`docker exec spark-master ... spark-submit`), không chạy PySpark từ Python trên Windows. Đường dẫn trong job là đường dẫn trong container (`/opt/app/...`).
- **Mật khẩu không viết trong code:** đọc từ biến môi trường (`os.environ`); mật khẩu chỉ gồm chữ và số.
- **Mỗi job streaming một thư mục checkpoint riêng**; không xóa checkpoint của job thật nếu không chủ động muốn xử lý lại.
- **Tên cột không trùng từ khóa SQL**; cột lấy từ metadata Kafka dùng tiền tố `kafka_`.
- **Trước khi chạy job mới**, xem http://localhost:8080 để chắc không còn ứng dụng cũ bị treo.
- **Script `.sh` luôn ở dạng LF**; script khởi tạo viết idempotent (chạy lại nhiều lần vẫn an toàn).
- **Dashboard Grafana luôn được xuất JSON** vào `infra/grafana/provisioning/dashboards/json/` và commit; panel chỉ dùng `SELECT`.