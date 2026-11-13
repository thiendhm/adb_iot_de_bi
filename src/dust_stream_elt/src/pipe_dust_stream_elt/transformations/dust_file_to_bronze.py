from pyspark import pipelines as dp
from pyspark.sql import functions as F

# 1. parameters come from the pipeline `configuration` block (resources/dust_stream.yml).
#    Inside the pipeline these keys are readable; on serverless interactive compute
#    non-allowlisted keys may raise, so fall back to the default there.
def _conf(key, default):
    try:
        return spark.conf.get(key, default)
    except Exception:
        return default

catalog = _conf("catalog", "adb_std")
bronze_table = _conf("bronze_table", "bronze.iot_raw_st")
landing_path = _conf("landing_path", "/Volumes/adb_std/default/raw_iot/opensense").rstrip("/")

bronze_3p = f"{catalog}.{bronze_table}"

# 2. raw JSON files -> bronze streaming table (owned by this pipeline).
#    Checkpoint and schema location are managed by the pipeline.
#    dust_brz_sil.py reads this table, so bronze always runs before silver in each update.
@dp.table(
    name=bronze_3p,
    comment="Raw openSenseMap JSON ingested with Auto Loader",
    cluster_by=["device_id", "_ingested_at"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def iot_raw_st():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.maxFilesPerTrigger", 500)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(landing_path)
        .withColumnsRenamed({
            "PM2.5": "PM2_5",
            "Temperatur": "Temperature",
            "rel. Luftfeuchte": "rel_Luftfeuchte",
        })
        .withColumns({
            "_ingested_at": F.current_timestamp(),
            "_source_file": F.col("_metadata.file_path"),
        })
    )