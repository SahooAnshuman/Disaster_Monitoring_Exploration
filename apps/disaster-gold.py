from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# ─── Spark Session Config ─────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("DisasterAlertGoldLayer")
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

# ─── Read Silver Stream ───────────────────────────────────────────
silver_stream = (
    spark.readStream
    .format("delta")
    .load("/opt/spark/warehouse/disaster_silver")
)

# ─── Dimension: Region ────────────────────────────────────────────
# Maps each region to its geographic type and inherent disaster risk
dim_region = silver_stream.select(
    "region"
).dropDuplicates() \
.withColumn(
    "region_type",
    when(col("region") == "COASTAL",   "Coastal Zone")
    .when(col("region") == "HILLSIDE", "Elevated Terrain")
    .when(col("region") == "FOREST",   "Forested Area")
    .when(col("region") == "URBAN",    "Urban Center")
    .when(col("region") == "RIVERBANK","Flood Plain")
    .otherwise("Arid Zone")
) \
.withColumn(
    "disaster_risk",
    when(col("region").isin("COASTAL", "RIVERBANK"),        "HIGH")
    .when(col("region").isin("HILLSIDE", "FOREST"),         "MEDIUM")
    .otherwise("LOW")
)

region_query = (
    dim_region.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/spark/warehouse/chk/dim_region")
    .option("path", "/opt/spark/warehouse/dim_region")
    .start()
)

# ─── Dimension: Station ───────────────────────────────────────────
# Maps each monitoring station to its coverage type and response time
dim_station = silver_stream.select(
    "station_id"
).dropDuplicates() \
.withColumn(
    "station_type",
    when(col("station_id").isin("DS100", "DS200"), "Primary Station")
    .when(col("station_id").isin("DS300", "DS400"), "Secondary Station")
    .otherwise("Remote Outpost")
) \
.withColumn(
    "response_time_mins",
    when(col("station_id").isin("DS100", "DS200"), 10)
    .when(col("station_id").isin("DS300", "DS400"), 25)
    .otherwise(45)
)

station_query = (
    dim_station.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/spark/warehouse/chk/dim_station")
    .option("path", "/opt/spark/warehouse/dim_station")
    .start()
)

# ─── Fact Table: Disaster Events ──────────────────────────────────
fact_stream = silver_stream.select(
    "sensor_id",
    "station_id",
    "region",
    "threat_index_int",
    "alert_level",
    "disaster_type",
    "threat_band",
    "critical_flag",
    "night_flag",
    "hour",
    "event_ts"
)

fact_enriched = fact_stream.withColumn("date", to_date("event_ts"))

fact_query = (
    fact_enriched.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/spark/warehouse/chk/fact_disaster")
    .option("path", "/opt/spark/warehouse/fact_disaster")
    .start()
)

spark.streams.awaitAnyTermination()