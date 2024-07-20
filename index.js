const express = require('express');
const { Kafka } = require('kafkajs');
const WebSocket = require('ws');

const app = express();
const port = 3000;

const kafka = new Kafka({
  clientId: 'event-planning-app',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'event-group' });

const wss = new WebSocket.Server({ port: 8080 });

const clients = [];

wss.on('connection', (ws) => {
  clients.push(ws);

  ws.on('close', () => {
    clients.splice(clients.indexOf(ws), 1);
  });
});

const broadcast = (data) => {
  clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
    }
  });
};

const run = async () => {
  await producer.connect();
  await consumer.connect();

  await consumer.subscribe({ topic: 'events', fromBeginning: true });

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = JSON.parse(message.value.toString());
      broadcast(event);
    },
  });

  app.use(express.json());

  app.post('/events', async (req, res) => {
    const event = req.body;
    await producer.send({
      topic: 'events',
      messages: [{ value: JSON.stringify(event) }],
    });
    res.status(200).send('Event created');
  });

  app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
  });
};

run().catch(console.error);
