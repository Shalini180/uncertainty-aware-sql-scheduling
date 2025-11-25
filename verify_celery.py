import os
import redis
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL")


def check_redis():
    print("Checking Redis Connectivity (Celery Broker)...")
    if not REDIS_URL:
        print("❌ REDIS_URL not found in environment.")
        return

    try:
        r = redis.from_url(REDIS_URL)
        r.ping()
        print("✅ Redis Connectivity Check Passed!")
    except Exception as e:
        print(f"❌ Redis Connectivity Check Failed: {e}")


if __name__ == "__main__":
    check_redis()
