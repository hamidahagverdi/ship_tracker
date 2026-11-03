from pyspark.sql import SparkSession, functions as F, types as T

spark = (
    SparkSession.builder.appName("ais_stream_to_bronze")
    .master("local[2]")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,"
            "org.apache.hadoop:hadoop-aws:3.4.1")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = T.StructType([
    T.StructField("mmsi", T.LongType()),
    T.StructField("ship_name", T.StringType()),
    T.StructField("lat", T.DoubleType()),
    T.StructField("lon", T.DoubleType()),
    T.StructField("speed_knots", T.DoubleType()),
    T.StructField("course", T.DoubleType()),
    T.StructField("heading", T.IntegerType()),
    T.StructField("nav_status", T.IntegerType()),
    T.StructField("time_utc", T.StringType()),
])

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "ship_positions")
    .option("startingOffsets", "earliest")
    .load()
)

bronze = (
    raw.select(
        F.from_json(F.col("value").cast("string"), schema).alias("d"),
        F.col("value").cast("string").alias("raw_json"),
        "partition", "offset",
        F.col("timestamp").alias("kafka_ts"),
    )
    .select("d.*", "raw_json", "partition", "offset", "kafka_ts")
    .withColumn("ingest_date", F.to_date("kafka_ts"))
)

query = (
    bronze.writeStream.format("parquet")
    .option("path", "s3a://bronze/ship_positions/")
    .option("checkpointLocation", "s3a://bronze/_checkpoints/ship_positions/")
    .partitionBy("ingest_date")
    .trigger(processingTime="1 minute")
    .start()
)

print("Spark streaming başladı. Hər 1 dəqiqədən bir MinIO-ya yazılacaq...")
query.awaitTermination()