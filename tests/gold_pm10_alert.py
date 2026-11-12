# check how many alert if threshold around 35
from pyspark.sql import functions as F, Window
import matplotlib.pyplot as plt

THRESHOLD = 35
BIN_WIDTH = 1.0

pm10 = (
    spark.table("adb_std.silver.iot_env_measurements")
    .filter(F.col("metric_name") == "PM10")
    .filter(F.col("value").isNotNull())
)

# Rolling 60-minute median using percentile_approx over a time-based window
window_60m = (
    Window.orderBy(F.col("measured_at").cast("long"))
    .rangeBetween(-3600, 0)  # 60 minutes = 3600 seconds
)

pm10_rolling = pm10.withColumn(
    "rolling_median_60m",
    F.percentile_approx("value", 0.5).over(window_60m)
)
pm10_threshold = pm10_rolling.filter(F.col("rolling_median_60m") > THRESHOLD)
# display(pm10_threshold.count())
pm10_rolling = pm10_rolling.withColumn("hour_window", F.date_trunc("hour", F.col("_ingested_at")))

# Keep only the first occurrence per day window (earliest _ingested_at)
window_first = Window.partitionBy("hour_window").orderBy("_ingested_at")
pm10_first = (
    pm10_rolling
    .withColumn("row_num", F.row_number().over(window_first))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
    .orderBy(F.col("_ingested_at").desc())
)
display(pm10_first)