from confluent_kafka import Consumer, TopicPartition
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("kinaction_consumer")

print(">> Инициализация Kafka Consumer")

# Конфигурация Kafka-консьюмера
consumer_config = {
    'bootstrap.servers': "localhost:29092,localhost:29093,localhost:29094",
    'group.id': 'kinaction_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

print(">> Создаем Consumer с конфигурацией:", consumer_config)
consumer = Consumer(consumer_config)

topic_name = "kinaction_alert"
print(f">> Подписываемся на топик: {topic_name}")
consumer.subscribe([topic_name])

keep_consuming = True

try:
    while keep_consuming:
        print(">> Ожидание сообщений (poll)...")
        record = consumer.poll(0.25)  # poll возвращает один объект Message или None

        if record is None:
            print(">> Нет новых сообщений...")
            continue

        if record.error():
            print(f">> Ошибка в сообщении: {record.error()}")
            continue

        print(f">> Принято сообщение: offset={record.offset()}, partition={record.partition()}")

        try:
            message_value = record.value().decode("utf-8")
            print(f">> Сообщение: {message_value}")
        except Exception as e:
            print(f">> Ошибка декодирования сообщения: {e}")
            continue

        offset_meta = record.offset() + 1
        ka_offset_list = [TopicPartition(topic_name, record.partition(), offset_meta)]  # Исправлено

        print(f">> Фиксируем offset: {offset_meta} для partition {record.partition()}")
        consumer.commit(offsets=ka_offset_list, asynchronous=False)  # Ожидается список

except KeyboardInterrupt:
    print(">> Остановка Consumer по Ctrl+C")

finally:
    print(">> Закрываем Consumer")
    consumer.close()
