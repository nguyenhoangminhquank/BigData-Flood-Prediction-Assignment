# Hợp đồng giao diện (Interface Contract)

Dự án: **Dự báo mực nước và nguy cơ ngập lụt từ dữ liệu cảm biến IoT**
Phiên bản: **v0.3**

Tài liệu này quy định dữ liệu trao đổi giữa các thành phần của hệ thống và giữa các thành viên. Mọi thay đổi phải được cập nhật vào đây và thông báo cho cả nhóm **trước khi** sửa code.

Trạng thái mỗi mục:

- ✅ **ĐÃ CHỐT**: viết code theo đúng mục này.
- ⏳ **CHƯA CHỐT**: dùng tạm giá trị ghi ở đây, sẽ thay đổi; người quyết định ghi rõ ở mục 10.

---

## 1. Quy ước chung ✅

| Quy ước | Nội dung |
|---|---|
| Thời gian | Theo **UTC**, chuẩn **ISO 8601**: `2022-08-15T00:00:00Z`; có mili giây khi cần: `2026-11-17T04:10:05.123Z`. Trong PostgreSQL dùng kiểu `TIMESTAMPTZ`, múi giờ máy chủ `UTC` |
| Tên cột / trường | Theo định dạng `snake_case`, đơn vị đặt ở cuối: `_mm`, `_m`, `_percent`, `_ms`. **Không** đặt tên trùng từ khóa SQL (`offset`, `partition`, `timestamp`, `key`…); cột lấy từ metadata Kafka thêm tiền tố `kafka_` (`kafka_key`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`) |
| Giá trị thiếu | Luôn là `null`. **Không** dùng `-999`, `NaN`, chuỗi rỗng hay giá trị giả khác |
| Số thực | `double` (PostgreSQL: `DOUBLE PRECISION`), dấu chấm thập phân (`41.2`) |
| Mã trạm | Giữ nguyên `station_id` của dữ liệu gốc, ví dụ `ASIA_0009` |
| Seed ngẫu nhiên | **`42`** cho mọi bước sinh dữ liệu ngẫu nhiên |
| Mã hóa văn bản | UTF-8 |

---

## 2. Môi trường và điểm kết nối ✅

Toàn bộ hạ tầng khởi chạy bằng `docker compose up -d` từ thư mục gốc dự án. Mật khẩu và tham số máy cục bộ đặt trong `.env` (không đưa lên Git); danh sách biến cần khai báo xem trong `.env.example`.

### 2.1. Phiên bản

| Thành phần | Phiên bản |
|---|---|
| Kafka (chế độ KRaft, 1 broker) | `apache/kafka:3.9.0` |
| Kafka UI | `provectuslabs/kafka-ui:v0.7.2` |
| PostgreSQL | `postgres:16` |
| Python (môi trường ảo của dự án) | **3.11** (PySpark 3.5 chưa hỗ trợ Python mới hơn) |
| Spark (standalone: 1 master + 1 worker) | Image riêng `floodprediction/spark:3.5.3`, build từ `apache/spark:3.5.3-scala2.12-java17-python3-ubuntu` (`infra/spark/Dockerfile`) |
| Python **bên trong** image Spark | 3.10 |
| Thư viện Python trong image Spark | `numpy==1.26.4`, `pandas==2.2.3`, `pyarrow==17.0.0`, `psycopg2-binary==2.9.9`, `pyyaml==6.0.2` |
| Connector của Spark (tải qua `spark.jars.packages`) | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`, `org.postgresql:postgresql:42.7.4` |
| Grafana | ⏳ Bổ sung khi dựng (bước 5) |

### 2.2. Địa chỉ kết nối

Chương trình **trong** mạng Docker (Spark, Grafana…) và chương trình **trên máy** (Producer, notebook…) dùng địa chỉ khác nhau:

| Dịch vụ | Từ trong Docker | Từ máy (Windows) |
|---|---|---|
| Kafka | `kafka:9092` (listener INTERNAL) | `localhost:9094` (listener EXTERNAL) |
| PostgreSQL | `postgres:5432` | `localhost:${POSTGRES_HOST_PORT}` (mặc định `5433`) |
| Kafka UI | — | http://localhost:8085 |
| Spark master | `spark://spark-master:7077` | Giao diện cụm: http://localhost:8080 |
| Spark worker | — | Giao diện worker: http://localhost:8081 |
| Ứng dụng Spark đang chạy (driver) | — | http://localhost:4040 (chỉ có khi đang có job; có tab Structured Streaming) |

### 2.3. Biến môi trường (`.env`)

| Biến | Ý nghĩa |
|---|---|
| `KAFKA_CLUSTER_ID` | Mã định danh cụm Kafka (sinh bằng `kafka-storage.sh random-uuid`) |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | Tài khoản và tên database |
| `POSTGRES_HOST_PORT` | Cổng PostgreSQL mở ra máy; đổi nếu cổng mặc định đã bị chiếm |

Các biến `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` cũng được truyền vào container `spark-master` để job Spark đọc bằng `os.environ[...]`. **Không viết mật khẩu trong code.**

### 2.4. Quy tắc chạy job Spark ✅

- **Mọi job Spark chạy bên trong container**, gửi từ `spark-master`:
  `docker exec [-it] spark-master /opt/spark/bin/spark-submit /opt/app/<thư-mục>/<file>.py`
  Không chạy PySpark từ Python trên Windows: driver và executor phải dùng **cùng phiên bản Python** (3.10 trong image).
- Dùng `-it` khi script cần nhập từ bàn phím (`input()`) hoặc khi cần dừng job streaming bằng `Ctrl+C`.
- **Đường dẫn trong job là đường dẫn bên trong container:**

| Trên máy | Trong container Spark (master và worker) |
|---|---|
| `./streaming` | `/opt/app/streaming` |
| `./common` | `/opt/app/common` |
| `./data` | `/opt/app/data` |

- **Cấu hình mặc định** đặt trong `infra/spark/spark-defaults.conf` (nằm trong image; sửa xong phải `docker compose build`):

| Cấu hình | Giá trị |
|---|---|
| `spark.master` | `spark://spark-master:7077` (không cần gõ `--master`) |
| `spark.jars.packages` | Hai connector ở mục 2.1 |
| `spark.jars.ivy` | `/tmp/.ivy2` (volume `spark_ivy`, lưu đệm thư viện đã tải) |
| `spark.sql.session.timeZone` | `UTC` |
| `spark.cores.max` | ⏳ Đề xuất `1`: mỗi ứng dụng tối đa 1 nhân để nhiều job chạy song song được; job streaming chính ghi đè bằng `--conf spark.cores.max=2` khi cần (xem mục 10) |

- Mỗi job gọi `spark.sparkContext.setLogLevel("WARN")` sau khi tạo SparkSession.
- **Job streaming:** mỗi job có thư mục checkpoint riêng `data/lake/_checkpoints/<tên-job>/`. Offset đã đọc từ Kafka lưu trong checkpoint, không lưu ở consumer group. Xóa checkpoint làm job đọc lại từ đầu topic và **ghi trùng** nếu đích không ghi idempotent: chỉ xóa khi chủ động muốn xử lý lại.

---

## 3. Bản tin cảm biến: topic `sensor-raw` ✅

Mỗi bản tin là **một lần đo trong một ngày của một trạm**, định dạng JSON.

| Trường | Kiểu | Null? | Ý nghĩa | Trạng thái |
|---|---|---|---|---|
| `message_id` | string | Không | Định danh duy nhất: `<station_id>_<YYYY-MM-DD>`. Dùng để loại trùng và đối soát | ✅ |
| `schema_version` | int | Không | Phiên bản schema, hiện tại `1` | ✅ |
| `station_id` | string | Không | Mã trạm; đồng thời là **key** của bản tin Kafka | ✅ |
| `event_time` | string (ISO 8601, UTC) | Không | **Thời điểm đo** (ngày trong dữ liệu). Dùng cho xử lý theo thời gian và watermark | ✅ |
| `sent_at` | string (ISO 8601, UTC, ms) | Không | **Thời điểm Producer gửi** (giờ thật). Dùng để đo độ trễ đầu–cuối | ✅ |
| `rainfall_mm` | double | Không | Lượng mưa ngày t. **Không gắn nhiễu** | ✅ |
| `rainfall_forecast_next` | double | Có | Lượng mưa "dự báo" ngày t+1 (phục vụ kịch bản B). Null ở ngày cuối của chuỗi | ✅ |
| `soil_moisture_percent` | double | Không | Độ ẩm đất (đặc trưng cho mô hình) | ✅ |
| `monsoon_season` | int (0/1) | Không | Mùa gió | ⏳ Chờ TV3 |
| `water_level_m` | double | **Có** | Mực nước **đã gắn nhiễu / lỗi**. Null khi mất gói | ✅ |

**Không có trong bản tin**:

- Nhãn lỗi cảm biến (`fault_type`): chỉ nằm trong file ground truth (mục 5), để bộ phát hiện lỗi không "nhìn thấy đáp án".
- Thông tin tĩnh của trạm (tên, lưu vực, quốc gia, tọa độ): lấy từ bảng `stations` khi cần (join).
- `flood_risk_score`, `severity_level`: rò rỉ nhãn, không dùng ở bất kỳ đâu trong pipeline.

**Ví dụ bản tin:**

```json
{
  "message_id": "ASIA_0009_2022-08-15",
  "schema_version": 1,
  "station_id": "ASIA_0009",
  "event_time": "2022-08-15T00:00:00Z",
  "sent_at": "2026-11-17T04:10:05.123Z",
  "rainfall_mm": 41.2,
  "rainfall_forecast_next": 37.9,
  "soil_moisture_percent": 63.5,
  "monsoon_season": 1,
  "water_level_m": null
}
```

### 3.1. Kiểm tra schema ⏳ (TV1, thực hiện ở bước Producer và Spark Streaming)

Kafka không tự kiểm tra nội dung bản tin, nên schema được thực thi ở hai đầu:

- **Schema máy đọc được:** `common/schemas/sensor_raw.v1.json` (chuẩn JSON Schema), là nguồn sự thật duy nhất cho mục 3.
- **Producer** kiểm tra từng bản tin với schema trước khi gửi; bản tin sai bị báo lỗi tại nguồn.
- **Spark** parse theo schema; bản tin không parse được đưa vào vùng **cách ly** (dead-letter), không làm dừng pipeline.
- Schema Registry (kiểm tra ngay tại Kafka) đưa vào hướng phát triển.

---

## 4. Topic Kafka ✅

| Topic | Key | Số partition | Replication | Giá trị | Ghi bởi | Đọc bởi |
|---|---|---|---|---|---|---|
| `sensor-raw` | `station_id` | 5 | 1 | Bản tin mục 3 | Producer (TV1) | Spark Streaming (TV1) |
| `flood-alerts` | `station_id` | 1 | 1 | Cảnh báo mục 4.1 | Spark Streaming (TV3) | Dashboard (TV2) |
| `sandbox` | tùy ý | 5 | 1 | Tùy ý | Bất kỳ ai | Bất kỳ ai |

- **Key = `station_id` là bắt buộc** với `sensor-raw` và `flood-alerts`: Kafka đưa mọi bản tin cùng key vào cùng một partition và giữ nguyên thứ tự **trong partition**. Kalman filter xử lý tuần tự theo thời gian của từng trạm nên cần đúng thứ tự này.
- Kafka **không** giữ thứ tự giữa các partition; xử lý theo thời gian phải dựa vào `event_time` (và watermark), không dựa vào thứ tự nhận bản tin.
- `sandbox` dành riêng cho thử nghiệm. **Không gửi dữ liệu thử vào `sensor-raw` hay `flood-alerts`.**
- Tắt tự tạo topic (`auto.create.topics.enable=false`). Ba topic trên được container `kafka-init` tạo tự động mỗi lần khởi động (`--if-not-exists`).
- Replication = 1 vì cụm chỉ có 1 broker (môi trường phát triển); không có dự phòng khi broker hỏng.
- Topic nội bộ `__consumer_offsets` (50 partition) do Kafka tự tạo để lưu offset của các consumer group; không đọc/ghi trực tiếp.

### 4.1. Bản tin cảnh báo: topic `flood-alerts` ⏳ (TV3 chốt)

| Trường | Kiểu | Ý nghĩa |
|---|---|---|
| `station_id` | string | Mã trạm |
| `event_time` | string (ISO 8601) | Ngày được dự báo ngập (t+1) |
| `scenario` | string | `A` hoặc `B` |
| `flood_prob` | double | Xác suất ngập do mô hình dự báo |
| `threshold` | double | Ngưỡng cảnh báo đang áp dụng |
| `created_at` | string (ISO 8601, ms) | Thời điểm sinh cảnh báo |

---

## 5. File ground truth (đánh giá lỗi cảm biến) ✅

- Do module nhiễu (TV1) sinh ra cùng file Parquet nhiễu, ở `data/noisy/`.
- **Chỉ dùng để đánh giá**; không bao giờ đưa vào luồng Kafka hay làm đầu vào mô hình.
- Khi đánh giá: nối (join) với kết quả tầng Silver theo `message_id`, rồi so `level_filtered` với `level_true` (RMSE) và `fault_flag` với `fault_type` (Precision/Recall).

| Trường | Kiểu | Ý nghĩa |
|---|---|---|
| `message_id` | string | Khóa để nối với bản tin |
| `station_id` | string | Mã trạm |
| `event_time` | string (ISO 8601) | Thời điểm đo |
| `level_true` | double | Mực nước gốc chưa nhiễu (`river_level_m`) |
| `level_noisy` | double (null được) | Giá trị đã gửi đi trong bản tin |
| `fault_type` | string | Một trong các giá trị ở mục 6 |
| `fault_id` | string (null được) | Mã của một **đợt lỗi** (ví dụ một đợt `stuck` kéo dài 10 ngày), để đánh giá theo sự kiện. Null khi `fault_type = none` |

---

## 6. Danh sách loại lỗi cảm biến (`fault_type`) ✅

| Giá trị | Mô tả | Nguyên nhân ngoài thực tế |
|---|---|---|
| `none` | Giá trị thật + nhiễu Gauss nhỏ | Sai số đo bình thường |
| `spike` | Một ngày nhảy vọt bất thường | Nhiễu điện, sét, vật trôi va vào cảm biến |
| `missing` | Một ngày bị null | Mất gói tin khi truyền |
| `dropout` | Null nhiều ngày liên tiếp | Hết pin, mất kết nối kéo dài |
| `stuck` | Giá trị đứng yên nhiều ngày | Cảm biến treo, gửi lặp giá trị cuối |
| `drift` | Giá trị lệch dần, rồi trở lại khi hiệu chuẩn | Cảm biến lão hóa, bám bẩn, lệch mốc |

**Quyết định ✅:** chỉ gắn nhiễu và lỗi cho `water_level_m`; `rainfall_mm` giữ nguyên, vì lượng mưa là biến điều khiển của Kalman filter. Cảm biến mưa bị nhiễu đưa vào hướng phát triển.

### 6.1. Tham số sinh lỗi ⏳ (TV1 + TV2 chốt sau khi chạy thử)

Lưu trong file cấu hình `common/noise_config.yaml`, **không viết cứng trong code**. Giá trị khởi điểm:

| Loại | Tham số | Giá trị khởi điểm |
|---|---|---|
| Nhiễu Gauss | độ lệch chuẩn σ | σ = 5 × (1 − `data_quality_score`), khoảng 0,05–0,6 m tùy trạm |
| `spike` | tỷ lệ ngày; độ lớn | 1% số ngày; ±(3–8) m |
| `missing` | tỷ lệ ngày | 3–5% số ngày |
| `dropout` | tần suất; độ dài | khoảng 1 đợt/trạm/năm; 3–15 ngày |
| `stuck` | tần suất; độ dài | khoảng 1 đợt/trạm/năm; 5–20 ngày |
| `drift` | số trạm; độ dài; độ lệch tối đa | khoảng 5/50 trạm; 30–90 ngày/đợt; lệch dần tới 1–2 m |

Mục tiêu tổng cộng khoảng **8–12% bản tin bất thường**, và mỗi loại lỗi có **ít nhất vài chục đợt** trong tập test 2022–2024 để Precision/Recall có ý nghĩa thống kê.

---

## 7. Hàm Kalman filter (TV2 viết, TV1 nhúng vào Spark) ✅

- Dạng hàm: nhận và trả về **pandas DataFrame của một trạm**, để dùng được với `applyInPandas` (batch) và `applyInPandasWithState` (streaming).
- **Đầu vào:** `station_id`, `event_time`, `rainfall_mm`, `water_level_m` (có thể null), sắp xếp tăng dần theo `event_time`.
- **Đầu ra:** giữ các cột đầu vào, thêm:

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `level_filtered` | double | Mực nước sau lọc (có giá trị cả khi đầu vào null) |
| `innovation` | double (null được) | Giá trị đo − giá trị bộ lọc dự đoán |
| `innovation_z` | double (null được) | Innovation chuẩn hóa theo độ lệch chuẩn dự đoán |
| `fault_flag` | int (0/1) | 1 nếu bộ phát hiện đánh dấu cảm biến bất thường |

- **Trạng thái giữ giữa các lô (streaming):** ước lượng `x`, phương sai `P`, số lần bất thường liên tiếp, `event_time` cuối cùng đã xử lý.
- **Tham số theo trạm** (`a`, `b`, `c`, `Q`, `R`, ngưỡng phát hiện): TV2 xuất ra file cấu hình; ước lượng **chỉ trên tập train 2000–2018**.

---

## 8. Cơ sở dữ liệu PostgreSQL ✅

Câu lệnh tạo bảng: `infra/postgres/init/01_schema.sql`, chạy tự động khi database được khởi tạo lần đầu (volume `pg_data` còn rỗng). Sửa file này sau khi database đã tồn tại thì **phải** tạo lại database (`docker compose down -v`, mất toàn bộ dữ liệu) hoặc chạy lại thủ công bằng `psql`.

### 8.1. Các bảng

| Bảng | Cột | Khóa chính | Khóa ngoại | Ghi bởi | Đọc bởi |
|---|---|---|---|---|---|
| `stations` | `station_id`, `station_name`, `basin`, `country`, `lat`, `lon` | `station_id` | — | TV1 (nạp từ `stations_clean.csv` của TV2) | Dashboard, Spark |
| `sensor_readings` | `station_id`, `event_time`, `rainfall_mm`, `level_raw`, `level_filtered`, `innovation`, `fault_flag`, `sent_at`, `ingested_at` | (`station_id`, `event_time`) | `station_id` → `stations` | Pipeline streaming (TV1 + hàm TV2) | Dashboard |
| `predictions` | `station_id`, `event_time`, `scenario`, `level_pred`, `flood_prob`, `created_at` | (`station_id`, `event_time`, `scenario`) | `station_id` → `stations` | TV3 | Dashboard |
| `flood_alerts` | `station_id`, `event_time`, `scenario`, `flood_prob`, `threshold`, `created_at` | (`station_id`, `event_time`, `scenario`) | `station_id` → `stations` | TV3 | Dashboard |
| `pipeline_metrics` | `id`, `batch_id`, `stage`, `records`, `throughput_rps`, `latency_ms`, `measured_at` | `id` (tự tăng) | — | TV1 | Báo cáo |

Ghi chú về cột:

- `event_time` trong `predictions` và `flood_alerts` là **ngày được dự báo** (t+1), không phải ngày chạy mô hình.
- `level_raw` = giá trị đo đã gắn nhiễu (`water_level_m` của bản tin), cho phép null.
- `sent_at` (từ bản tin) và `ingested_at` (database tự điền `now()`) dùng để tính độ trễ: `ingested_at − sent_at`.
- `stage` trong `pipeline_metrics`: `bronze`, `silver` hoặc `gold`, để đo riêng từng tầng.

### 8.2. Ràng buộc dữ liệu

| Ràng buộc | Nội dung |
|---|---|
| `CHECK` | `lat` ∈ [−90, 90]; `lon` ∈ [−180, 180]; `rainfall_mm` ≥ 0; `fault_flag` ∈ {0, 1}; `scenario` ∈ {`A`, `B`}; `flood_prob`, `threshold` ∈ [0, 1]; `records` ≥ 0 |
| Khóa ngoại | Chỉ ghi được dữ liệu của trạm đã có trong `stations` |
| Khóa chính | Không có hai dòng trùng (trạm, thời điểm[, kịch bản]) |

### 8.3. Chỉ mục

| Chỉ mục | Phục vụ truy vấn |
|---|---|
| Khóa chính của từng bảng (tự động) | Theo trạm và khoảng thời gian |
| `idx_readings_event_time` | Tất cả các trạm tại một thời điểm (bản đồ dashboard) |
| `idx_readings_faults` (partial, `fault_flag = 1`) | Danh sách cảm biến bất thường |
| `idx_predictions_event_time` | Dự báo theo ngày |
| `idx_alerts_created_at` (giảm dần) | Cảnh báo mới nhất |

### 8.4. Quy tắc ghi dữ liệu

- **Ghi idempotent:** mọi thành phần ghi vào `sensor_readings`, `predictions`, `flood_alerts` dùng `INSERT ... ON CONFLICT DO NOTHING` (hoặc `DO UPDATE` khi cần ghi đè), để xử lý lại một lô sau sự cố không tạo bản ghi trùng.
- **Thứ tự nạp:** bảng `stations` phải có đủ 50 trạm **trước khi** pipeline chạy, do ràng buộc khóa ngoại.
- **Thứ tự xóa:** xóa bảng con (`sensor_readings`, `predictions`, `flood_alerts`) trước, bảng `stations` sau.
- Cẩn thận với `docker compose down -v`: lệnh này xóa volume `pg_data`, tức là xóa toàn bộ dữ liệu database.

---

## 9. Lưu trữ file ✅

| Đường dẫn | Nội dung | Định dạng |
|---|---|---|
| `data/raw/` | `station_daily_data_full.csv` gốc | CSV, phân cách `;` |
| `data/raw/stations_clean.csv` | 50 trạm đã sửa tọa độ, quốc gia (TV2 cung cấp) | CSV |
| `data/noisy/` | Dữ liệu nhiễu + ground truth | Parquet |
| `data/lake/bronze/` | Bản tin thô từ Kafka | Parquet, partition theo `basin` |
| `data/lake/silver/` | Dữ liệu đã lọc Kalman + cờ lỗi | Parquet, partition theo `basin` |
| `data/lake/gold/` | Dự báo, cảnh báo | Parquet, partition theo `basin` |
| `data/lake/_checkpoints/<tên-job>/` | Checkpoint của từng job Spark Streaming (offset đã xử lý) | Do Spark quản lý |
| `infra/postgres/init/` | Script SQL khởi tạo database | SQL |
| `infra/spark/` | `Dockerfile` và `spark-defaults.conf` của image Spark | Dockerfile, properties |
| `common/schemas/` | Schema máy đọc được của các topic | JSON Schema |

Thư mục `data/` không đưa lên Git; mỗi thành viên tự đặt file gốc và tự sinh lại dữ liệu nhiễu nhờ seed cố định.

---

## 10. Các điểm chưa chốt

| # | Nội dung | Người quyết định | Hạn |
|---|---|---|---|
| 1 | `monsoon_season`: gửi kèm bản tin hay Spark tự suy ra từ tháng + lưu vực | TV3 | Mốc 1 |
| 2 | Tham số sinh lỗi (mục 6.1) | TV1 + TV2 | Sau lần chạy thử module nhiễu |
| 3 | Schema chi tiết topic `flood-alerts` (mục 4.1) | TV3 | Mốc 3 |
| 4 | Định dạng file `stations_clean.csv`; sau đó TV1 viết `infra/postgres/init/02_seed_stations.sql` để nạp tự động | TV2 → TV1 | Mốc 1 |
| 5 | File `common/schemas/sensor_raw.v1.json` (mục 3.1) | TV1 | Bước Producer |
| 6 | Phiên bản và điểm kết nối Grafana (mục 2) | TV1 | Bước 5 |
| 7 | Áp dụng `spark.cores.max` (mục 2.4) và số nhân cho job streaming chính | TV1 | Bước 8 |

---

## 11. Lịch sử thay đổi

| Phiên bản | Ngày | Người sửa | Nội dung |
|---|---|---|---|
| v0.1 | 17/11/2026 | TV1 | Bản đầu tiên |
| v0.2 | 17/11/2026 | TV1 | Thêm mục 2 (môi trường, điểm kết nối, biến `.env`); thêm mục 3.1 (kiểm tra schema); thêm topic `sandbox`, ghi chú thứ tự partition và `kafka-init`; mở rộng mục 8 (PostgreSQL): thêm cột `sent_at`, `stage`, `id`, khóa chính, khóa ngoại, ràng buộc CHECK, chỉ mục, quy tắc ghi idempotent; đánh số lại các mục |
| v0.3 | 24/11/2026 | TV1 | Thêm Spark vào mục 2.1, 2.2; thêm mục 2.4 (quy tắc chạy job Spark, đường dẫn trong container, cấu hình mặc định, checkpoint); quy ước tên cột không trùng từ khóa SQL, tiền tố `kafka_` (mục 1); thêm đường dẫn checkpoint và `infra/spark/` (mục 9); cập nhật mục 10 |