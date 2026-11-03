import boto3
from pyspark.sql import SparkSession, functions as F

# silver bucket-i yoxdursa, yarat
s3 = boto3.client("s3", endpoint_url="http://localhost:9000",
                  aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin")
if "silver" not in [b["Name"] for b in s3.list_buckets()["Buckets"]]:
    s3.create_bucket(Bucket="silver")

spark = (
    SparkSession.builder.appName("bronze_to_silver")
    .master("local[2]")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
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

bronze = spark.read.parquet("s3a://bronze/ship_positions/")
bronze_count = bronze.count()

# Qayda 7: region (producer-dəki koordinatlarla eyni)
region = (
    F.when(F.col("lat").between(32, 49) & F.col("lon").between(-125, -117), "US_WEST")
     .when(F.col("lat").between(24, 31) & F.col("lon").between(-98, -81), "US_GULF")
     .when(F.col("lat").between(25, 45) & F.col("lon").between(-81, -66), "US_EAST")
     .when(F.col("lat").between(36.5, 47.2) & F.col("lon").between(46.5, 55.2), "CASPIAN")
     .otherwise("OTHER")
)

# Qayda 3: "2026-11-03 09:10:30.827050407 +0000 UTC" → mikrosaniyəyə qədər kəs → timestamp
ts_text = F.regexp_extract("time_utc", r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,6})?)", 1)

clean_name = F.regexp_replace(F.trim("ship_name"), r"\s+", " ")

silver = (
    bronze
    # Qayda 1 və 2
    .filter(F.col("mmsi").isNotNull() & F.col("lat").isNotNull() & F.col("lon").isNotNull())
    .filter(F.col("lat").between(-90, 90) & F.col("lon").between(-180, 180))
    # Qayda 3
    .withColumn("event_ts", ts_text.cast("timestamp"))
    # Qayda 4
    .withColumn("ship_name", F.when(clean_name == "", None).otherwise(clean_name))
    # Qayda 5 və 6
    .withColumn("speed_knots", F.when(F.col("speed_knots") >= 102.3, None).otherwise(F.col("speed_knots")))
    .withColumn("heading", F.when(F.col("heading") == 511, None).otherwise(F.col("heading")))
    # Qayda 7
    .withColumn("region", region)
    .withColumn("event_date", F.to_date("event_ts"))
    # Qayda 8
    .dropDuplicates(["mmsi", "event_ts"])
    .select("mmsi", "ship_name", "region", "lat", "lon", "speed_knots",
            "course", "heading", "nav_status", "event_ts", "event_date", "kafka_ts")
)

silver.write.mode("overwrite").partitionBy("event_date").parquet("s3a://silver/ship_positions/")

# Keyfiyyət hesabatı
result = spark.read.parquet("s3a://silver/ship_positions/")
silver_count = result.count()
print("\n=== Keyfiyyət hesabatı ===")
print(f"Bronze: {bronze_count} sətir")
print(f"Silver: {silver_count} sətir  (atılan: {bronze_count - silver_count})")
print("\nSilver-də NULL sayı:")
result.select([F.sum(F.col(c).isNull().cast("int")).alias(c)
               for c in ["ship_name", "speed_knots", "heading", "event_ts"]]).show()
print("Region üzrə:")
result.groupBy("region").count().orderBy(F.desc("count")).show()

spark.stop()
