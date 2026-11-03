from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.appName("explore_bronze")
    .master("local[1]")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("s3a://bronze/ship_positions/")

print("\n=== 1. Ümumi say ===")
print("Sətir sayı:", df.count())
print("Unikal gəmi sayı:", df.select("mmsi").distinct().count())

print("\n=== 2. Sxem ===")
df.printSchema()

print("\n=== 3. Nümunə sətirlər ===")
df.select("mmsi", "ship_name", "lat", "lon", "speed_knots", "heading", "time_utc").show(5, truncate=False)

print("\n=== 4. Hər sütunda NULL sayı ===")
cols = ["mmsi", "ship_name", "lat", "lon", "speed_knots", "course", "heading", "nav_status", "time_utc"]
df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).show()

print("\n=== 5. Gizli boş dəyərlər ===")
df.select(
    F.sum((F.trim("ship_name") == "").cast("int")).alias("bos_ad"),
    F.sum((F.col("speed_knots") >= 102.3).cast("int")).alias("suret_102_3"),
    F.sum((F.col("heading") == 511).cast("int")).alias("heading_511"),
).show()

print("\n=== 6. Dublikatlar (eyni gəmi, eyni vaxt) ===")
total = df.count()
unique = df.dropDuplicates(["mmsi", "time_utc"]).count()
print(f"Cəmi: {total}, unikal: {unique}, dublikat: {total - unique}")

print("\n=== 7. Ən çox siqnal göndərən 10 gəmi ===")
df.groupBy("mmsi", "ship_name").count().orderBy(F.desc("count")).show(10, truncate=False)

print("\n=== 8. Region üzrə (uzunluğa görə) ===")
df.withColumn("region", F.when(F.col("lon") > 0, "CASPIAN").otherwise("US")) \
  .groupBy("region").count().show()

spark.stop()
