from io import BytesIO

import pandas as pd
from minio import Minio


BUCKET_NAME = "smart-farming"
GOLD_OBJECT = "gold/smart-farming/anomaly_results.parquet"


client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password123",
    secure=False,
)


response = client.get_object(
    BUCKET_NAME,
    GOLD_OBJECT,
)

try:
    dataframe = pd.read_parquet(
        BytesIO(response.read())
    )
finally:
    response.close()
    response.release_conn()


print("\nShape:")
print(dataframe.shape)

print("\nAnomaly distribution:")
print(dataframe["anomaly_label"].value_counts())

print("\nStrongest anomalies:")
print(
    dataframe[
        dataframe["anomaly_label"] == "anomaly"
    ]
    .sort_values("anomaly_score")
    .head(10)
    [
        [
            "farm_id",
            "anomaly_score",
            "soil_moisture_%",
            "temperature_C",
            "rainfall_mm",
            "humidity_%",
            "NDVI_index",
        ]
    ]
    .to_string(index=False)
)