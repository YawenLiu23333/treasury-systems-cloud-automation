import requests
from src.config import settings

def send_alert(message: str):
    if not settings.ALERT_WEBHOOK_URL:
        return
    try:
        requests.post(settings.ALERT_WEBHOOK_URL, json={"env": settings.ENV, "message": message}, timeout=5)
    except Exception:
        pass
