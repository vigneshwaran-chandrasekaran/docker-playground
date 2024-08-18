const express = require('express');
const bodyParser = require('body-parser');
const { Kafka } = require('kafkajs');

const app = express();
const port = 3000;

app.use(bodyParser.json());

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'test-group' });

app.post('/send', async (req, res) => {
  const message = req.body.message;

  await producer.connect();
  await producer.send({
    topic: 'test-topic',
    messages: [{ value: message }],
  });
  await producer.disconnect();

  res.send('Message sent to Kafka');
});

const runConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      });
    },
  });
};

runConsumer().catch(console.error);

app.listen(port, () => {
  console.log(`Express server listening at http://localhost:${port}`);
});
