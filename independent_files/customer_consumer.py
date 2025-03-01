from confluent_kafka import Consumer
import logging
import threading

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("HelloWorldConsumer")

# Настройки Kafka
kafka_config = {
    "bootstrap.servers": "localhost:29092,localhost:29093,localhost:29094",
    "group.id": "kinaction_helloconsumer",
    "enable.auto.commit": True,
    "auto.commit.interval.ms": 1000,
    "key.deserializer": str,
    "value.deserializer": str,
    "auto.offset.reset": "earliest"
}
# "group.id" — идентификатор группы потребителей Kafka (Consumer Group).
# "kinaction_helloconsumer" — все консьюмеры с таким group.id будут работать вместе, обрабатывая сообщения из разделов (партиций) топика.
# "enable.auto.commit" — если True, Kafka автоматически сохраняет смещение (offset) сообщений, которые уже обработаны.
# "auto.commit.interval.ms" — интервал в миллисекундах для автоматического коммита смещения сообщений. Здесь установлено 1000, то есть каждую 1 секунду.
# "key.deserializer" и "value.deserializer" — функции для десериализации ключей и значений сообщений. Указано str, то есть данные будут интерпретироваться как строки.
# "auto.offset.reset" — определяет, с какого места начинать чтение, если ранее смещения не сохранялись.
# "earliest" — начинает чтение с самых старых сообщений в топике.

consumer = Consumer({
    "bootstrap.servers": kafka_config["bootstrap.servers"],
    "group.id": kafka_config["group.id"],
    "enable.auto.commit": kafka_config["enable.auto.commit"],
    "auto.offset.reset": "earliest"
})

keep_consuming = True


def consume():
    consumer.subscribe(["kinaction_helloworld"])
    while keep_consuming:
        messages = consumer.poll(1.0)
        if messages is None:
            continue
        if messages.error():
            log.error(f"Consumer error: {messages.error()}")
            continue
        log.info(f"kinaction_info offset = {messages.offset()}, kinaction_value = {messages.value().decode('utf-8')}")
    consumer.close()


if __name__ == "__main__":
    thread = threading.Thread(target=consume, daemon=True)
    thread.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        keep_consuming = False
        log.info("Shutting down consumer...")
