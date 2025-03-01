from confluent_kafka import Producer
from envparse import Env

env = Env()
env.read_envfile()

KAFKA1: str = env.str("KAFKA1")
KAFKA2: str = env.str("KAFKA2")
KAFKA3: str = env.str("KAFKA3")


kafka_config = {
    "bootstrap.servers": f"{KAFKA1},{KAFKA2},{KAFKA3}",
    "key.serializer": str.encode,
    "value.serializer": str.encode
}

# Создание продюсера
producer = Producer({"bootstrap.servers": kafka_config["bootstrap.servers"]})


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
