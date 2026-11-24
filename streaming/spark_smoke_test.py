from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .appName("spark-smoke-test")
         .getOrCreate())

# 1. Tạo 1 triệu dòng giả, chia thành 4 partition
df = (spark.range(0, 1_000_000, numPartitions=8)
      .withColumn("station", F.concat(F.lit("ST_"), (F.col("id") % 5).cast("string")))
      .withColumn("value", F.rand(seed=42) * 10))

# 2. Gom nhóm: buộc Spark shuffle dữ liệu giữa các task
result = (df.groupBy("station")
            .agg(F.count("*").alias("n"), F.round(F.avg("value"), 3).alias("avg_value"))
            .orderBy("station"))
result.show()

# 3. Ghi Parquet vào thư mục dùng chung
result.write.mode("overwrite").parquet("/opt/app/data/lake/_smoke_test")
print("Số partition ban đầu:", df.rdd.getNumPartitions())

input("Mở http://localhost:4040 để xem, nhấn Enter để kết thúc...")
spark.stop()