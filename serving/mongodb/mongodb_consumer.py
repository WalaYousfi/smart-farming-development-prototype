from kafka import KafkaConsumer
from pymongo import MongoClient
import json

mongo_client = MongoClient("mongodb://127.0.0.1:27017")
db = mongo_client["company"]
collection = db["employees"]

print("Mongo version:", mongo_client.server_info()["version"])

print("Current count before consuming:", collection.count_documents({}))

collection.insert_one({"test": "mongo works"})
print("Test count:", collection.count_documents({}))


consumer = KafkaConsumer(
    "users",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="python-user-consumer-debug",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Consumer started. Waiting for messages...")

for message in consumer:
    user = message.value

    result = collection.update_one(
        {"id": user["id"]},
        {"$set": user},
        upsert=True
    )

    print(
        f"Saved user {user['id']} | "
        f"matched={result.matched_count} | "
        f"upserted={result.upserted_id} | "
        f"count={collection.count_documents({})}"
    )