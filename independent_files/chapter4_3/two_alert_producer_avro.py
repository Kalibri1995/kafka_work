import json
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ModernAvroProducer")


def alert_to_dict(alert, ctx):
    return alert


class AlertProducer:
    def __init__(self):
        schema_registry_conf = {
            'url': 'http://localhost:28081'
        }

        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        with open("alert.avsc", "r") as f:
            schema_str = f.read()

        self.value_serializer = AvroSerializer(
            schema_registry_client=schema_registry_client,
            schema_str=schema_str,
            to_dict=alert_to_dict
        )

        producer_conf = {
            'bootstrap.servers': 'localhost:29092',
            'key.serializer': StringSerializer('utf_8'),
            'value.serializer': self.value_serializer,
            'acks': 'all'
        }

        self.producer = SerializingProducer(producer_conf)

    def send_alert(self, topic, alert):
        def delivery_report(err, msg):
            if err is not None:
                logger.error(f"❌ Ошибка доставки: {err}")
            else:
                logger.info(f"✅ Отправлено в {msg.topic()} [offset={msg.offset()}]")

        self.producer.produce(
            topic=topic,
            key=alert["stageId"],
            value=alert,
            on_delivery=delivery_report
        )
        self.producer.flush()


if __name__ == "__main__":
    producer = AlertProducer()
    alert = {
        "alertId": 1,
        "stageId": "Stage-1",
        "alertLevel": "CRITICAL",
        "alertMessage": "Stage stopped unexpectedly"
    }
    producer.send_alert("kinaction_alerttrend_avro", alert)
