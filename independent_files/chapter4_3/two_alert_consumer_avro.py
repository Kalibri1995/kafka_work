from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka import DeserializingConsumer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AlertAvroConsumer")

class AlertAvroConsumer:
    def __init__(self):
        schema_registry_conf = {'url': 'http://localhost:28081'}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        avro_deserializer = AvroDeserializer(schema_registry_client)

        self.consumer = DeserializingConsumer({
            'bootstrap.servers': 'localhost:29092',
            'key.deserializer': StringDeserializer('utf_8'),
            'value.deserializer': avro_deserializer,
            'group.id': 'alert-avro-group',
            'auto.offset.reset': 'earliest',
        })

        self.consumer.subscribe(['kinaction_alerttrend_avro'])

    def listen(self):
        logger.info("🎧 Слушаем сообщения...")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue

                value = msg.value()
                if value is None:
                    continue

                stage_id = value.get('stageId')
                alert_level = value.get('alertLevel')
                message = value.get('alertMessage')

                logger.info(f"🔔 {stage_id} [{alert_level}]: {message}")
        except KeyboardInterrupt:
            logger.info("Остановка консюмера...")
        finally:
            self.consumer.close()


if __name__ == '__main__':
    consumer = AlertAvroConsumer()
    consumer.listen()
