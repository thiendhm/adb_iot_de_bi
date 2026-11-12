# 🚦 [AZ_ADB_IOT] Dust IOT Lakehouse Pipeline

An end to end Medallion data pipeline from api to take action on threshold violation of dust

## Mission

- In factory environment, **Dust over threshold** and spike cause **high defect cost** as well as automatic trigger **false alarm**
- This pipeline aims to use **minimal resources** to alert per aligned timeframe on **smooth sensor data**.
- It starts from collect **api iot data**, process it with traceability , calculate technical value and trigger alert to email on threshold violation.

## 📍 Architecture

```mermaid

flowchart LR

    A[Dust iot api] -->|Azure function| B[(JSON Data Lake ADLS2)]
    B -->|PySpark/Delta| C[Bronze: append_reading_per_device]

    C -->|PySpark / Delta| D[(Silver: dedup_append_reading_per_sensor)]

    D -->|Aggregations| E[(Gold: alert_timeframe_rolling_value)]

    click B "src/dust_stream_elt/src/pipe_dust_stream_elt" "Inspect Schedule IOT to Storage"
    click C "src/dust_stream_elt/src/pipe_dust_stream_elt" "Inspect Bronze Ingestion"

    click D "src/dust_stream_elt/src/pipe_dust_stream_elt" "Inspect Silver Transformation"

    click E "src/dust_stream_elt/src/pm10_hour_threshold.dbalert.json" "Inspect Gold Aggregation"

```

![Result](assets/mail_alert.png)

![Pipeline Overview](assets/pipeline_graph.png)

  ---
## 🔍 Key thoughts

| Stage  | Input  |Plan  | Thoughts  | Code Reference |
| --- | ---| --- | --- | ---|
| **Source** | data update every 3 min <br>from https://api.opensensemap.org/ | 1. Get data by period <br>2. Land to data lake                                                  | 1. API update lag from measured time<br>2. Align with alert timeframe to save cost   | [Azure_Function](src/AzFxIngestAPI) |
| **Bronze** | JSON files periodly  | 1. append raw to delta format<br>2. add metadata   | 1. reduce cost re-read vast json files<br>2. create single copy of source change  | [Bronze Code](src/dust_stream_elt/src/pipe_dust_stream_elt)|
| **Silver** | Raw ingest data including history                              | 1. explode JSON array to long format<br>2. clean data ready for analytics                       | 1. analytics use need long tabular format<br>per measurement type ( humid, PM10 dust)<br>2. pipeline stable per re-run with deduplicate | [Silver Code](src/dust_stream_elt/src/pipe_dust_stream_elt)|
| **Gold**   | Long multi measurement big data                                | 1. tranform into needed data with rolling<br>by alert timeframe                                 | 1. apply logic to serve business case<br>2. reduce cost on self-serve analytics                                                         | [Gold Code](src/dust_stream_elt/src/pipe_dust_stream_elt) |
| **Alert**  | Aggregated information periodly                                | 1. set alert for over theshold using SQL<br>2. send business formatted mail <br>for take action | 1. need actionable trigger to serve business<br> | [Alert Code](src/dust_stream_elt/src/pm10_hour_threshold.dbalert.json) |
