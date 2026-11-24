from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("kafka-batch-test").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (spark.read
       .format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", "sandbox")
       .option("startingOffsets", "earliest")
       .load())

raw.printSchema()

(raw.select(F.col("key").cast("string").alias("key"),
            "partition", "offset", "timestamp",
            F.col("value").cast("string").alias("value"))
    .orderBy("partition", "offset")
    .show(truncate=False))
input("Mở http://localhost:4040 để xem, nhấn Enter để kết thúc...")

spark.stop()