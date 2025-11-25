import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")


def check_db():
    print("Checking Database Connectivity...")
    if not DB_URL:
        print("❌ DATABASE_URL not found in environment.")
        return

    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("✅ DB Connectivity Check Passed!")
            print(f"   Result: {result.scalar()}")
    except Exception as e:
        print(f"❌ DB Connectivity Check Failed: {e}")


if __name__ == "__main__":
    check_db()
