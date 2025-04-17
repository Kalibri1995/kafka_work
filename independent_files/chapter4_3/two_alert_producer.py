import logging
from confluent_kafka import Producer, KafkaException
from two_alert import Alert
from two_alert_key_serde import AlertKeySerializer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AlertProducer")


class AlertTrendingProducer:
    def __init__(self):
        self.config = {
            'bootstrap.servers': 'localhost:29092,localhost:29093,localhost:29094',
            'acks': 'all',
            'retries': 3,
            'max.in.flight.requests.per.connection': 1
        }
        self.producer = Producer(self.config)
        self.key_serializer = AlertKeySerializer()

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Ошибка доставки: {err}")
        else:
            logger.info(f"✔ offset = {msg.offset()}, topic = {msg.topic()}, timestamp = {msg.timestamp()}")

    def send_alert(self, topic: str, alert: Alert):
        key_bytes = self.key_serializer.serialize(alert)
        value_str = alert.get_alert_message()

        try:
            self.producer.produce(
                topic=topic,
                key=key_bytes,
                value=value_str,
                callback=self.delivery_report
            )
            self.producer.flush()
        except KafkaException as e:
            logger.error(f"Kafka error: {e}")

    def close(self):
        self.producer.flush()
        logger.info("Производитель закрыт.")


# Пример использования
if __name__ == "__main__":
    alert = Alert(1, "Stage 1", "CRITICAL", "Overheating")
    producer = AlertTrendingProducer()
    producer.send_alert("kinaction_alerttrend", alert)
    producer.close()