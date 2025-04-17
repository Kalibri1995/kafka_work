import logging
from confluent_kafka import Producer, KafkaException


class AuditProducer:
    def __init__(self, bootstrap_servers: str = "localhost:29092,localhost:29093,localhost:29094"):
        # Настройка логгера
        self.log = logging.getLogger("AuditProducer")
        logging.basicConfig(level=logging.INFO)

        # Конфигурация Kafka-производителя
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'acks': 'all',  # Подтверждение от всех реплик
            'retries': 3,  # Повторы при ошибках
            'max.in.flight.requests.per.connection': 1  # Последовательная отправка
        }

        # Создание Kafka-производителя
        self.producer = Producer(self.config)

    def delivery_report(self, err, msg):
        """Вызывается при подтверждении доставки сообщения."""
        if err is not None:
            self.log.error(f"Ошибка доставки сообщения: {err}")
        else:
            self.log.info(
                f"Доставлено сообщение в тему: {msg.topic()}, "
                f"раздел: {msg.partition()}, offset: {msg.offset()}, "
                f"timestamp: {msg.timestamp()}"
            )

    def send(self, topic: str, value: str, key: str = None):
        """
        Отправляет сообщение в Kafka и ждет подтверждения.
        :param topic: Название топика
        :param value: Сообщение
        :param key: Ключ сообщения (опционально)
        """
        try:
            self.producer.produce(
                topic=topic,
                key=key,
                value=value,
                callback=self.delivery_report
            )
            self.producer.flush()  # Ждем подтверждения доставки
        except KafkaException as e:
            self.log.error(f"Ошибка Kafka: {e}")
        except Exception as e:
            self.log.error(f"Общая ошибка при отправке: {e}")

    def close(self):
        """Закрывает producer и освобождает ресурсы."""
        self.producer.flush()
        self.log.info("Kafka-производитель закрыт.")


# Пример использования
if __name__ == "__main__":
    audit_producer = AuditProducer()
    audit_producer.send("kinaction_info", "Turn sensor ON4")
    audit_producer.close()
