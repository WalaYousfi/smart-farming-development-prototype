import { Kafka } from "kafkajs";
import fs from "fs";
import express, { json } from "express";
import { MongoClient } from "mongodb";

const app = express();

const kafka = new Kafka({
  clientId: "api-producer",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();
app.use(express.json());

const mongo = new MongoClient("mongodb://127.0.0.1:27017");

//Create POST /users
app.post("/users", async (req, res) => {
  const user = req.body;

  await producer.send({
    topic: "users",
    messages: [
      {
        key: String(user.id),
        value: JSON.stringify(user),
      },
    ],
  });

  res.json({
    message: "User sent to Kafka",
    user,
  });
});

// Bulk Upload Endpoint
app.post("/users/bulk", async (req, res) => {
  const raw = fs.readFileSync(
    new URL("./MOCK_DATA.json", import.meta.url),
    "utf8",
  );

  const users = JSON.parse(raw);

  const messages = users.map((user) => ({
    key: String(user.id),
    value: JSON.stringify(user),
  }));

  await producer.send({
    topic: "users",
    messages,
  });

  res.json({
    inserted: users.length,
  });
});

// Create GET Endpoint
app.get("/users", async (req, res) => {
  const users = await mongo
    .db("company")
    .collection("employees")
    .find()
    .limit(20)
    .toArray();

  res.json(users);
});

async function start() {
  await producer.connect();
  await mongo.connect();

  app.listen(3000, () => {
    console.log("API running on port 3000");
  });
}

start();
