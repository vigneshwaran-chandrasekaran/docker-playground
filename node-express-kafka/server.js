const express = require("express");
const kafka = require("kafka-node");

const app = express();
const port = 3301;

// Kafka client setup
const client = new kafka.KafkaClient({ kafkaHost: "kafka:9092" });
const producer = new kafka.Producer(client);

producer.on("ready", function () {
  console.log("Kafka Producer is connected and ready.");
});

producer.on("error", function (err) {
  console.error("Kafka Producer error:", err);
});

app.get("/", (req, res) => {
  const payloads = [{ topic: "test-topic", messages: "Hello Kafka" }];
  producer.send(payloads, (err, data) => {
    if (err) {
      console.error("Error sending message to Kafka:", err);
      res.status(500).send("Error sending message to Kafka");
    } else {
      console.log("Message sent to Kafka:", data);
      res.send("Message sent to Kafka");
    }
  });
});

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`);
});
