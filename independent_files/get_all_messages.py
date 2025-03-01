# 🔥 Что делает этот код?
# ✅ Сбрасывает offset на 0 для всех партиций (читает ВСЕ сообщения).
# ✅ Использует assign() вместо subscribe(), чтобы вручную выбрать смещения.
# ✅ Читает все сообщения и сохраняет их в all_messages.
# ✅ Останавливается, когда сообщений больше нет (msg is None).
# ✅ Выводит все сообщения после завершения.


from confluent_kafka import Consumer, TopicPartition
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("kinaction_consumer")

print(">> Инициализация Kafka Consumer")

consumer_config = {
    'bootstrap.servers': "localhost:29092,localhost:29093,localhost:29094",
    'group.id': 'kinaction_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

print(">> Создаем Consumer с конфигурацией:", consumer_config)
consumer = Consumer(consumer_config)

topic_name = "kinaction_alert"

# Получаем информацию о партициях
print(f">> Запрашиваем партиции для топика: {topic_name}")
metadata = consumer.list_topics(topic_name)
partitions = metadata.topics[topic_name].partitions.keys()

# Назначаем Consumer ко всем партициям с offset=0
topic_partitions = [TopicPartition(topic_name, p, 0) for p in partitions]
consumer.assign(topic_partitions)

print(f">> Назначены партиции: {list(partitions)}")

all_messages = []

try:
    while True:
        print(">> Ожидание сообщений (poll)...")
        msg = consumer.poll(1.0)  # Ждем 1 сек

        if msg is None:  # Если новых сообщений нет — выходим
            print(">> Достигнут конец топика, выходим.")
            break

        if msg.error():
            print(f">> Ошибка в сообщении: {msg.error()}")
            continue

        message_value = msg.value().decode("utf-8")
        print(f">> Принято сообщение: offset={msg.offset()}, partition={msg.partition()}, value={message_value}")

        all_messages.append(message_value)

except KeyboardInterrupt:
    print(">> Остановка Consumer по Ctrl+C")

finally:
    print(f">> Получено {len(all_messages)} сообщений")
    consumer.close()
    print(">> Consumer закрыт")

# Если нужно использовать сообщения дальше
print(">> Все сообщения получены:", all_messages)
