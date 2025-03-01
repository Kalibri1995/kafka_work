from fastapi import APIRouter
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
from config import kafka_config

consumer_router = APIRouter()

# Создаем Kafka-консьюмера
consumer = Consumer({
    "bootstrap.servers": kafka_config["bootstrap.servers"],
    "group.id": "kinaction_consumer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})


@consumer_router.get("/{topic_name}/consume_one")
def consume_one_message(topic_name: str):
    """Читает одно сообщение из указанного топика."""
    consumer.subscribe([topic_name])
    consumer.poll(0.5)  # Даем Kafka время на назначение партиций

    # Получаем назначенные партиции
    partitions = consumer.assignment()
    if not partitions:
        return {"status": "No partitions assigned"}

    # Устанавливаем оффсет в начало
    for p in partitions:
        p.offset = OFFSET_BEGINNING  # ✅ Используем корректное значение
        consumer.seek(p)

    msg = consumer.poll(1.0)  # Пробуем читать сообщение

    if msg is None or msg.error():
        return {"status": "No messages available"}

    return {
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "key": msg.key().decode() if msg.key() else None,
        "value": msg.value().decode() if msg.value() else None
    }


@consumer_router.get("/{topic_name}/consume_all")
def consume_all_messages(topic_name: str):
    """Читает все доступные сообщения из топика."""
    consumer.subscribe([topic_name])
    messages = []

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            break  # Нет новых сообщений

        messages.append({
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "key": msg.key().decode() if msg.key() else None,
            "value": msg.value().decode() if msg.value() else None
        })

    return {"messages": messages}


@consumer_router.get("/{topic_name}/consume_from_offset/{offset}")
def consume_from_offset(topic_name: str, offset: int):
    """Читает сообщения из топика начиная с указанного оффсета."""
    # Получаем все партиции
    metadata = consumer.list_topics(topic_name)
    partitions = [p.id for p in metadata.topics[topic_name].partitions.values()]

    # Устанавливаем оффсет для всех партиций
    assigned_partitions = [TopicPartition(topic_name, p, offset) for p in partitions]
    consumer.assign(assigned_partitions)

    messages = []
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            break  # Нет новых сообщений

        messages.append({
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "key": msg.key().decode() if msg.key() else None,
            "value": msg.value().decode() if msg.value() else None
        })

    return {"messages": messages}
