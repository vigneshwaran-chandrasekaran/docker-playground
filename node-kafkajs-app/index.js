const express = require("express");
const { Kafka } = require("kafkajs");

// Initialize Kafka Client
const kafka = new Kafka({
  clientId: "express-app",
  brokers: ["localhost:9092"], // Kafka broker running on localhost
});

// Initialize Producer and Consumer
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "test-group" });

const app = express();
app.use(express.json());

// API to send a message to Kafka
app.post("/send", async (req, res) => {
  const { message } = req.body;

  // Connect to producer and send message to Kafka topic
  await producer.connect();
  await producer.send({
    topic: "test-topic",
    messages: [{ value: message }],
  });

  res.send("Message sent to Kafka");
});

// API to start consuming Kafka messages
app.get("/consume", async (req, res) => {
  await consumer.connect();
  await consumer.subscribe({ topic: "test-topic", fromBeginning: true });

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      });
    },
  });

  res.send("Consumer started");
});

app.listen(3303, () => {
  console.log("Server is running on port 3303");
});

// Start the consumer when the application starts
(async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "test-topic", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      });
    },
  });
})();
