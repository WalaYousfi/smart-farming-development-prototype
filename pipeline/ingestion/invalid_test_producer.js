import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { Kafka } from "kafkajs";

const currentFile = fileURLToPath(import.meta.url);
const currentDirectory = path.dirname(currentFile);

const dataPath = path.resolve(
  currentDirectory,
  "../../data/test/invalid_field_readings.json"
);

const kafka = new Kafka({
  clientId: "invalid-agricultural-test-producer",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();

async function main() {
  const rawContent = fs.readFileSync(dataPath, "utf-8");
  const records = JSON.parse(rawContent);

  console.log(`Loaded ${records.length} controlled test records`);

  await producer.connect();
  console.log("Connected to Kafka");

  const messages = records.map((record, index) => ({
    key: `invalid-test-${Date.now()}-${index + 1}`,
    value: JSON.stringify(record),
  }));

  await producer.send({
    topic: "raw-field-readings",
    messages,
  });

  console.log(
    `Sent ${messages.length} records to raw-field-readings`
  );
}

main()
  .catch((error) => {
    console.error("Test producer failed:", error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await producer.disconnect();
    console.log("Producer disconnected");
  });