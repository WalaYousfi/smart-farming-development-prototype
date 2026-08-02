import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { Kafka } from "kafkajs";


const currentFile = fileURLToPath(import.meta.url);
const currentDirectory = path.dirname(currentFile);

const WEATHER_DATA_PATH = path.resolve(
  currentDirectory,
  "../../data/source/weather/weather_observations.json"
);

const KAFKA_BROKER =
  process.env.KAFKA_SERVER || "localhost:9092";

const WEATHER_TOPIC =
  process.env.WEATHER_KAFKA_TOPIC ||
  "raw-weather-readings";


const kafka = new Kafka({
  clientId: "farm-weather-station-producer",
  brokers: [KAFKA_BROKER],
});

const producer = kafka.producer({
  allowAutoTopicCreation: false,
});


function loadWeatherRecords() {
  if (!fs.existsSync(WEATHER_DATA_PATH)) {
    throw new Error(
      `Weather data file not found: ${WEATHER_DATA_PATH}`
    );
  }

  const fileContent = fs.readFileSync(
    WEATHER_DATA_PATH,
    "utf-8"
  );

  const records = JSON.parse(fileContent);

  if (!Array.isArray(records)) {
    throw new Error(
      "Weather data must contain a JSON array."
    );
  }

  if (records.length === 0) {
    throw new Error(
      "Weather data file contains no records."
    );
  }

  return records;
}


function validateMinimumFields(record, index) {
  const requiredFields = [
    "weather_station_id",
    "farm_id",
    "observed_at",
  ];

  const missingFields = requiredFields.filter(
    (field) =>
      record[field] === undefined ||
      record[field] === null ||
      String(record[field]).trim() === ""
  );

  if (missingFields.length > 0) {
    throw new Error(
      `Weather record ${index + 1} is missing: ` +
      missingFields.join(", ")
    );
  }
}


async function main() {
  const records = loadWeatherRecords();

  records.forEach((record, index) => {
    validateMinimumFields(record, index);
  });

  console.log(
    `Loaded ${records.length} weather observations`
  );

  console.log(`Kafka broker: ${KAFKA_BROKER}`);
  console.log(`Kafka topic: ${WEATHER_TOPIC}`);

  await producer.connect();

  console.log("Weather producer connected to Kafka");

  const messages = records.map((record) => ({
    key: `${record.farm_id}:${record.weather_station_id}`,
    value: JSON.stringify(record),
    headers: {
      source_system: "farm_weather_station",
      source_format: "json",
      schema_version: "weather-source-1.0.0",
    },
  }));

  const result = await producer.send({
    topic: WEATHER_TOPIC,
    messages,
  });

  console.log(
    `Sent ${messages.length} weather observations`
  );

  console.log("Kafka result:", result);
}


main()
  .catch((error) => {
    console.error(
      "Weather producer failed:",
      error
    );

    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await producer.disconnect();

      console.log(
        "Weather producer disconnected"
      );
    } catch (disconnectError) {
      console.error(
        "Could not disconnect producer:",
        disconnectError
      );
    }
  });