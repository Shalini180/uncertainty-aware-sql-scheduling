import os
from celery import Celery

# Get Redis URL from environment, default to localhost for local dev
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery_app = Celery(
    "energy_ml_scheduler",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["src.scheduler.tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)
