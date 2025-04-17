from confluent_kafka import Consumer, KafkaError
import logging
from two_alert_key_serde import AlertKeyDeserializer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AlertTrendingConsumer")


class AlertTrendingConsumer:
    def __init__(self, group_id='alert-group'):
        self.config = {
            'bootstrap.servers': 'localhost:29092,localhost:29093,localhost:29094',
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }
        self.consumer = Consumer(self.config)
        self.key_deserializer = AlertKeyDeserializer()

    def subscribe_and_poll(self, topic):
        self.consumer.subscribe([topic])
        logger.info(f"Подписались на топик: {topic}")

        try:
            while True:
                msg = self.consumer.poll(1.0)  # ждём 1 секунду
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue  # достигли конца раздела
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        continue

                key = self.key_deserializer.deserialize(msg.key())
                value = msg.value().decode('utf-8')  # так как значение — строка

                logger.info(f"Alert from Stage {key}: {value}")
        except KeyboardInterrupt:
            logger.info("Остановка консюмера")
        finally:
            self.consumer.close()
            logger.info("Consumer закрыт")

if __name__ == '__main__':
    consumer = AlertTrendingConsumer()
    consumer.subscribe_and_poll("kinaction_alerttrend")
