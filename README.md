# Smart Farming Data Lake Prototype

## Baseline V1

This version implements a basic Medallion data-lake pipeline.

### Architecture

CSV → Kafka → MinIO Bronze → Pandas Silver
→ Isolation Forest → MinIO Gold

### Dataset

Smart_Farming_Crop_Yield_2024.csv

### Expected results

- Source records: 500
- Silver records: 500
- Normal records: 475
- Anomalous records: 25

### MinIO locations

- Bronze: bronze/smart-farming/
- Silver: silver/smart-farming/cleaned_field_readings.parquet
- Gold: gold/smart-farming/anomaly_results.parquet

### Run order

1. Start Docker services.
2. Start the Bronze consumer.
3. Run the Kafka producer.
4. Run the Silver processor.
5. Run the Gold anomaly-detection job.
6. Inspect Silver and Gold results.