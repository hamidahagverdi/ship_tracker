import boto3
from pyspark.sql import SparkSession, functions as F

s3 = boto3.client("s3", endpoint_url="http://localhost:9000",
                  aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin")
if "gold" not in [b["Name"] for b in s3.list_buckets()["Buckets"]]:
    s3.create_bucket(Bucket="gold")

spark = (
    SparkSession.builder.appName("silver_to_gold")
    .master("local[2]")
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.1,org.postgresql:postgresql:42.7.4")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

silver = spark.read.parquet("s3a://silver/ship_positions/")

# nav_status: 1 = lövbərdə, 5 = limanda bağlanıb
anchored = F.col("nav_status").isin(1, 5)
moving = F.col("speed_knots") > 0.5

# Gold 1: gün + region
daily_region_stats = (
    silver.groupBy("event_date", "region")
    .agg(
        F.countDistinct("mmsi").alias("ships"),
        F.count("*").alias("signals"),
        F.countDistinct(F.when(anchored, F.col("mmsi"))).alias("ships_anchored_or_moored"),
        F.countDistinct(F.when(moving, F.col("mmsi"))).alias("ships_moving"),
        F.round(F.avg(F.when(moving, F.col("speed_knots"))), 2).alias("avg_speed_moving"),
    )
    .orderBy("event_date", "region")
)

# Gold 2: gün + gəmi
ship_daily_activity = (
    silver.groupBy("event_date", "mmsi")
    .agg(
        F.max("ship_name").alias("ship_name"),
        F.max("region").alias("region"),
        F.count("*").alias("signals"),
        F.min("event_ts").alias("first_seen"),
        F.max("event_ts").alias("last_seen"),
        F.round(F.max("speed_knots"), 1).alias("max_speed"),
        F.round(F.avg("speed_knots"), 1).alias("avg_speed"),
    )
    .withColumn("active_minutes",
        F.round((F.unix_timestamp("last_seen") - F.unix_timestamp("first_seen")) / 60, 1))
)

# MinIO-ya yaz
daily_region_stats.write.mode("overwrite").parquet("s3a://gold/daily_region_stats/")
ship_daily_activity.write.mode("overwrite").parquet("s3a://gold/ship_daily_activity/")

# Postgres-ə yaz
jdbc_url = "jdbc:postgresql://localhost:5432/ships"
props = {"user": "admin", "password": "admin", "driver": "org.postgresql.Driver"}
daily_region_stats.write.jdbc(jdbc_url, "daily_region_stats", mode="overwrite", properties=props)
ship_daily_activity.write.jdbc(jdbc_url, "ship_daily_activity", mode="overwrite", properties=props)

print("\n=== daily_region_stats ===")
daily_region_stats.show(truncate=False)
print("=== Ən aktiv 5 gəmi ===")
ship_daily_activity.orderBy(F.desc("signals")).show(5, truncate=False)

spark.stop()
