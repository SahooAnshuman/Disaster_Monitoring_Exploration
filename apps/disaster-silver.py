from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ─── Spark Session Config ─────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("DisasterAlertSilverLayer")
    # cluster master
    .master("spark://spark-master:7077")
    # delta lake
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ─── Read Bronze Stream ───────────────────────────────────────────
bronze_df = (
    spark.readStream
    .format("delta")
    .load("/opt/spark/warehouse/disaster_bronze")
)

# ─── Data Quality Flag ────────────────────────────────────────────
dq_df = bronze_df.withColumn(
    "dq_flag",
    when(col("sensor_id").isNull(),             "MISSING_SENSOR")
    .when(col("event_time").isNull(),           "MISSING_TIME")
    .when(col("raw_json").contains("CORRUPTED"),"CORRUPT_JSON")
    .otherwise("OK")
)

# ─── Safe Type Casting ────────────────────────────────────────────
# threat_index was kept as StringType in bronze to absorb dirty values
# now we safely cast to int — dirty strings like "LOW" will become null
typed = dq_df.withColumn(
    "threat_index_int",
    col("threat_index").cast("int")
).withColumn(
    "event_ts",
    to_timestamp("event_time")
)

# ─── Business Validation Rules ────────────────────────────────────
validated = typed.withColumn(
    "threat_valid",
    when(
        (col("threat_index_int") >= 0) & (col("threat_index_int") <= 100), 1
    ).otherwise(0)
).withColumn(
    "time_valid",
    when(
        col("event_ts") <= current_timestamp() + expr("INTERVAL 10 MINUTES"), 1
    ).otherwise(0)
)

# ─── Filter Good Records ──────────────────────────────────────────
clean_stream = validated.filter(
    (col("dq_flag")      == "OK") &
    (col("threat_valid") == 1)    &
    (col("time_valid")   == 1)
)

# ─── Handle Late Data ─────────────────────────────────────────────
# Disaster alerts are time-critical — 15 min watermark same as traffic
watermarked = clean_stream.withWatermark("event_ts", "15 minutes")

# ─── Deduplication ────────────────────────────────────────────────
deduped = watermarked.dropDuplicates(
    ["sensor_id", "event_ts"]
)

# ─── Feature Engineering ──────────────────────────────────────────
silver_final = (
    deduped
    .withColumn("hour", hour("event_ts"))

    # Threat band based on standard disaster alert scale
    .withColumn("threat_band",
        when(col("threat_index_int") <= 20, "LOW")
        .when(col("threat_index_int") <= 40, "MODERATE")
        .when(col("threat_index_int") <= 60, "HIGH")
        .when(col("threat_index_int") <= 80, "SEVERE")
        .otherwise("CATASTROPHIC")
    )

    # Critical flag — triggers emergency response if SEVERE or above
    .withColumn("critical_flag",
        when(col("threat_index_int") >= 61, 1).otherwise(0)
    )

    # Night vulnerability flag — disasters at night are harder to respond to
    .withColumn("night_flag",
        when(
            (col("hour") >= 22) | (col("hour") <= 5), 1
        ).otherwise(0)
    )
)

# ─── Write Silver Table ───────────────────────────────────────────
silver_query = (
    silver_final.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/spark/warehouse/chk/disaster_silver")
    .option("path", "/opt/spark/warehouse/disaster_silver")
    .start()
)

spark.streams.awaitAnyTermination()