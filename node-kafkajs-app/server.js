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

const topic = "topic-test";
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "test-group" });

const run = async () => {
  // Producing
  await producer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    // eachBatch: async ({ batch }) => {
    //   console.log(batch)
    // },
    eachMessage: async ({ topic, partition, message }) => {
      console.log("kafka consumer message received");
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      console.log(`- ${prefix} ${message.key}#${message.value}`);
    },
  });

  app.get("/", (req, res) => {
    res.send(`Hello World! from port=${port}`);
  });

  app.post("/send", async (req, res) => {
    const { topic, message } = req.body;
    console.log("topic", topic);
    console.log("message", message);
    if (!topic || !message) {
      return res.status(400).send("Topic and message are required");
    }
    try {
      await producer.send({
        topic: topic,
        messages: [{ value: message }],
      });
      res.status(200).send("Message sent to Kafka");
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
