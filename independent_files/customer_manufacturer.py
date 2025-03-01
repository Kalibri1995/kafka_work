from confluent_kafka import Producer


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


# Настройки Kafka
kafka_config = {
    "bootstrap.servers": "localhost:29092,localhost:29093,localhost:29094",
    "key.serializer": str.encode,
    "value.serializer": str.encode
}

# Создание продюсера
producer = Producer({"bootstrap.servers": kafka_config["bootstrap.servers"]})

topic = "kinaction_helloworld"
message = "hello world again1!"

# Отправка сообщения
producer.produce(topic, key=None, value=message, callback=delivery_report)

# Ожидание завершения отправки
producer.flush()
