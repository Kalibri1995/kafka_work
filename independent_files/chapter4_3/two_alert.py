class Alert:
    def __init__(self, alert_id: int, stage_id: str, alert_level: str, alert_message: str):
        self.alert_id = alert_id
        self.stage_id = stage_id
        self.alert_level = alert_level
        self.alert_message = alert_message

    def get_stage_id(self):
        return self.stage_id

    def get_alert_message(self):
        return self.alert_message
