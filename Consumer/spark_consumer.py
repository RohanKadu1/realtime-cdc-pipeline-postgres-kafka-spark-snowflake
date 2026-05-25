from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys
import os

# ----------------------------
# 1. Spark Session
# ----------------------------
spark = SparkSession.builder \
    .appName("CDC_Kafka_to_Snowflake") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# 2. Schema
# ----------------------------
schema = StructType([
    StructField("action", StringType()),
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("updated_at", StringType()),
    StructField("lsn_num", IntegerType())
])

print("✅ Starting Spark Stream...", file=sys.stderr)

# ----------------------------
# 3. Read Kafka
# ----------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", os.getenv("KAFKA_TOPIC")) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# ----------------------------
# 4. Parse JSON
# ----------------------------
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# ----------------------------
# ✅ 5. Snowflake Config
# ----------------------------
snowflake_options = {
    "sfURL": os.getenv("SF_URL"),
    "sfUser": os.getenv("SF_USER"),
    "sfPassword": os.getenv("SF_PASSWORD"),
    "sfDatabase": os.getenv("SF_DATABASE"),
    "sfSchema": os.getenv("SF_SCHEMA"),
    "sfWarehouse": os.getenv("SF_WAREHOUSE"),
    "sfRole": os.getenv("SF_ROLE")
}

# ----------------------------
# 6. Write Function
# ----------------------------
def write_to_snowflake(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    print(f"✅ Writing batch {batch_id} to Snowflake", file=sys.stderr)

    batch_df.write \
        .format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "USERS_CDC_STAGING") \
        .mode("append") \
        .save()

# ----------------------------
# ✅ 7. Start Streaming
# ----------------------------
query = parsed_df.writeStream \
    .foreachBatch(write_to_snowflake) \
    .option("checkpointLocation", "/opt/spark/checkpoints/snowflake_pipeline") \
    .start()

query.awaitTermination()