from kafka import KafkaProducer
import json

class Alert:
    def __init__(self, alert_id, stage, level, message):
        self.alert_id = alert_id
        self.stage = stage
        self.level = level
        self.message = message

    def get_alert_message(self):
        return self.message

    def to_dict(self):
        return {
            "alert_id": self.alert_id,
            "stage": self.stage,
            "level": self.level,
            "message": self.message
        }

def on_send_success(record_metadata):
    print(f"Message sent to {record_metadata.topic} "
          f"partition {record_metadata.partition} "
          f"offset {record_metadata.offset}")

def on_send_error(excp):
    print(f"Message failed to send: {excp}")

# Создаем продюсера Kafka
producer = KafkaProducer(
    bootstrap_servers=["kafka1:9092", "kafka2:9093", "kafka3:9094"],  # Укажи свой адрес брокера Kafka
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Создаем alert
alert = Alert(1, "Stage 1", "CRITICAL", "Stage 1 stopped")

# Создаем сообщение
topic = "kinaction_alert"
message_value = alert.to_dict()
message_key = alert.get_alert_message().encode("utf-8")

# Отправляем сообщение
future = producer.send(topic, key=message_key, value=message_value)
future.add_callback(on_send_success)
future.add_errback(on_send_error)

# Закрываем продюсера
producer.flush()
producer.close()
