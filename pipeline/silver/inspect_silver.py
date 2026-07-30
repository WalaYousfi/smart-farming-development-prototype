from io import BytesIO

from minio import Minio
import pandas as pd


client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password123",
    secure=False
)

response = client.get_object(
    "smart-farming",
    "silver/smart-farming/cleaned_field_readings.parquet"
)

parquet_data = BytesIO(response.read())

df = pd.read_parquet(parquet_data)

response.close()
response.release_conn()

print("\nShape:")
print(df.shape)

print("\nColumns:")
print(df.columns.tolist())

print("\nFirst rows:")
print(df.head())

print("\nMissing values:")
print(df.isnull().sum())

print("\nData types:")
print(df.dtypes)

print("\nQuality status:")
print(df["data_quality_status"].value_counts())