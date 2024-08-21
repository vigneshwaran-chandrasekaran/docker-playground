const express = require("express");
const { Kafka } = require("kafkajs");

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
console.log("one isSubscribed", isSubscribed);

const run = async () => {
  // start consuming Kafka messages
  await producer.connect();
  console.log("two isSubscribed", isSubscribed);
  const topics = ["topic-test", "consume-topic"];
  // List of topics to subscribe

  if (!isSubscribed) {
    // Check if already subscribed

    await consumer.subscribe({
      topics,
      fromBeginning: true,
    });
    isSubscribed = true; // Set flag after subscribing

    console.log(`Subscribed to topics: ${topics}`);
  } else {
    console.log("Already subscribed to the topic.");
  }
  console.log("three isSubscribed", isSubscribed);
  await consumer.run({
    eachBatch: async ({ batch }) => {
      console.log(batch);
    },
    eachMessage: async ({ topic, partition, message }) => {
      if (topic === "topic-test") {
        console.log("1111111111111 topic-test");
      } else if (topic === "consume-topic") {
        console.log("22222222222 consume-topic");
      } else {
        console.log("unknown topic", topic);
      }
      console.log("kafka consumer message received", message);
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      console.log(`- ${prefix} ${message.key}#${message.value}`);
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
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
        topic: topic,
        messages: [{ value: message }],
      });
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
