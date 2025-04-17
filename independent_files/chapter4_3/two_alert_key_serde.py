import json

class AlertKeySerializer:
    def serialize(self, alert: 'Alert') -> bytes:
        if alert is None:
            return None
        return alert.get_stage_id().encode('utf-8')

class AlertKeyDeserializer:
    def deserialize(self, data: bytes) -> str:
        if data is None:
            return None
        return data.decode('utf-8')
