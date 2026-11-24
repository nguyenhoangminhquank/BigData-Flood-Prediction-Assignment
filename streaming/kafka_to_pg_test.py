import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# --- Kết nối PostgreSQL: lấy từ biến môi trường, không viết cứng mật khẩu ---
PG_URL = f"jdbc:postgresql://postgres:5432/{os.environ['POSTGRES_DB']}"
PG_PROPS = {
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver": "org.postgresql.Driver",
}

# --- Schema của bản tin trong topic sandbox ---
schema = StructType([
    StructField("station_id", StringType()),
    StructField("event_time", TimestampType()),
])

spark = SparkSession.builder.appName("kafka-to-pg-test").getOrCreate()

# 1. Nguồn: đọc LIÊN TỤC từ Kafka
stream = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")
          .option("subscribe", "sandbox")
          .option("startingOffsets", "earliest")
          .load())

# 2. Biến đổi: đổi byte → chuỗi → parse JSON theo schema
parsed = (stream
          .select(F.col("key").cast("string").alias("kafka_key"),
                  "partition", "offset",
                  F.from_json(F.col("value").cast("string"), schema).alias("msg"))
          .select("kafka_key", "partition", "offset", "msg.*")
          .withColumn("ingested_at", F.current_timestamp()))

# 3. Đích: xử lý từng micro-batch
def write_batch(batch_df, batch_id):
    batch_df.persist()
    n = batch_df.count()
    print(f"[batch {batch_id}] nhận {n} bản tin")
    if n > 0:
        batch_df.write.mode("append").parquet("/opt/app/data/lake/_sandbox_test")
        batch_df.write.mode("append").jdbc(PG_URL, "sandbox_test", properties=PG_PROPS)
    batch_df.unpersist()

query = (parsed.writeStream
         .foreachBatch(write_batch)
         .option("checkpointLocation", "/opt/app/data/lake/_checkpoints/sandbox_test")
         .trigger(processingTime="5 seconds")
         .start())

query.awaitTermination()