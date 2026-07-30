from io import BytesIO

from minio import Minio
import pandas as pd
from sklearn.ensemble import IsolationForest
from pathlib import Path
import joblib
import os
from dotenv import load_dotenv

load_dotenv()


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

client = Minio(
     MINIO_ENDPOINT,
      access_key=MINIO_ACCESS_KEY,
      secret_key=MINIO_SECRET_KEY,
      secure=False,
)

response = client.get_object(
    "smart-farming",
    "silver/smart-farming/cleaned_field_readings.parquet"
)

df = pd.read_parquet(
    BytesIO(response.read())
)

response.close()
response.release_conn()


features = [

    "soil_moisture_%",
    "soil_pH",
    "temperature_C",
    "rainfall_mm",
    "humidity_%",
    "sunlight_hours",
    "pesticide_usage_ml",
    "NDVI_index"
]

X = df[features].to_numpy()


model = IsolationForest(

    n_estimators=200,
    contamination=0.05,
    random_state=42
)

df["anomaly"] = model.fit_predict(X)

df["anomaly_score"] = model.decision_function(X)

df["anomaly_label"] = (
    df["anomaly"]
    .map({
        1: "normal",
        -1: "anomaly"
    })
)

print(
    df["anomaly_label"]
    .value_counts()
)


# Inspect anomalies
anomalies = (
    df[df["anomaly_label"] == "anomaly"]
    .sort_values("anomaly_score")
)

print(
    anomalies[
        [
            "farm_id",
            "soil_moisture_%",
            "soil_pH",
            "temperature_C",
            "rainfall_mm",
            "humidity_%",
            "sunlight_hours",
            "pesticide_usage_ml",
            "NDVI_index",
            "anomaly_score",
        ]
    ].head(25).to_string(index=False)
)

gold_buffer = BytesIO()

df.to_parquet(
    gold_buffer,
    index=False
)

gold_buffer.seek(0)

client.put_object(

    "smart-farming",

    "gold/smart-farming/anomaly_results.parquet",

    gold_buffer,

    length=len(
        gold_buffer.getvalue()
    )
)

print("Gold uploaded")