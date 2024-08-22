const express = require("express");
const { Kafka } = require("kafkajs");
const { nanoid } = require("nanoid");

const app = express();
const port = 3302;

app.use(express.json());

// Kafka setup
const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["kafka:9092"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "test-group" });
let isSubscribed = false; // Flag to track subscription

const run = async () => {
  // start consuming Kafka messages
  await producer.connect();
  const topics = ["topic-test", "consume-topic"];
  // List of topics to subscribe

  // Check if already subscribed
  if (!isSubscribed) {
    await consumer.subscribe({
      topics,
      fromBeginning: true,
    });
    isSubscribed = true; // Set flag after subscribing
    console.log(`Subscribed to topics: ${topics}`);
  } else {
    console.log("Already subscribed to the topics.");
  }
  await consumer.run({
    eachBatch: async ({ batch }) => {
      console.log("batch", batch);
    },
    eachMessage: async ({ topic, partition, message }) => {
      if (topic === "topic-test") {
        console.log("topic-test received");
      } else if (topic === "consume-topic") {
        console.log("consume-topic received");
      } else {
        console.log("unknown topic", topic);
      }
      console.log("kafka consumer message received", JSON.stringify(message));
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      console.log(`- ${prefix} ${message.key}#${message.value}`);
      console.log({
        topic, // Identify from which topic the message comes from
        partition,
        offset: message.offset,
        key: message.key ? message.key.toString() : null,
        value: message.value.toString(),
        headers: message.headers,
      });
    },
  });

  app.get("/", (req, res) => {
    res.send(`Hello World! from port=${port}`);
  });

  app.post("/send", async (req, res) => {
    // Producing
    const { topic, message, timestamp } = req.body;
    console.log("topic", topic);
    console.log("message", message);
    console.log("timestamp", timestamp);
    if (!topic || !message) {
      return res.status(400).send("Topic and message are required");
    }
    try {
      await producer.send({
        topic,
        messages: [
          {
            key: nanoid(),
            value: JSON.stringify(message),
            headers: {
              correlationId: "12345",
              headerKey1: nanoid(),
              headerKey2: "headerValue2",
            },
          },
          {
            key: nanoid(),
            value: JSON.stringify(message),
            headers: {
              correlationId: "6789",
              headerKey1: nanoid(),
              headerKey2: "headerValue2",
            },
          },
        ],
      });
      console.log("Messages sent successfully!");
      res.status(200).json({
        topic,
        message,
        timestamp,
      });
    } catch (err) {
      console.error("Error sending message to Kafka:", err);
      res.status(500).send("Error sending message to Kafka");
    }
  });

  app.listen(port, () => {
    console.log(`Example app listening at http://localhost:${port}`);
  });
};

run().catch(console.error);
