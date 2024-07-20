const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'test-producer',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

const run = async () => {
  await producer.connect();
  await producer.send({
    topic: 'events',
    messages: [{ value: 'Hello KafkaJS user!' }]
  });

  await producer.disconnect();
};

run().catch(console.error);
