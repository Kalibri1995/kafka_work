from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import requests
import json

# 🔹 Проверяем доступность Schema Registry
SCHEMA_REGISTRY_URL = "http://localhost:8081"
try:
    response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=5)
    response.raise_for_status()
    print("✅ Schema Registry доступен")
except requests.exceptions.RequestException as e:
    print(f"❌ Ошибка: Schema Registry недоступен ({e})")
    exit(1)

# 🔹 Загружаем Avro-схему
SCHEMA_FILE = "../alert-schema.avsc"
try:
    with open(SCHEMA_FILE, "r") as f:
        schema_str = f.read()
except FileNotFoundError:
    print(f"❌ Файл схемы {SCHEMA_FILE} не найден!")
    exit(1)

# 🔹 Подключаем Schema Registry
schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# 🔹 Настраиваем Kafka Producer
producer_config = {
    "bootstrap.servers": "localhost:29092,localhost:29093,localhost:29094",  # Убедись, что Kafka работает
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": avro_serializer,
}

producer = SerializingProducer(producer_config)

# 🔹 Данные для отправки
alert_data = {
    "id": 1,
    "message": "Test alert",
    "timestamp": "2025-03-21T12:00:00Z"
}

# 🔹 Отправка сообщения в Kafka
topic = "kinaction_schematest"
try:
    producer.produce(
        topic=topic,
        key=str(alert_data["id"]),
        value=alert_data,
        on_delivery=lambda err, msg: print(f"✅ Отправлено в {msg.topic()}") if not err else print(f"❌ Ошибка: {err}")
    )
    producer.flush()
except Exception as e:
    print(f"❌ Ошибка при отправке в Kafka: {e}")
