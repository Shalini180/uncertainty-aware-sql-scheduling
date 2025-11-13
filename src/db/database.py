"""Database connection and session management"""

import os
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool
from contextlib import contextmanager
from .models import Base

# Get database URL from environment
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://admin:changeme123@localhost:5432/energy_ml"
)

# Create engine with appropriate settings
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,  # Verify connections before using
    pool_size=10,
    max_overflow=20,
    echo=os.getenv("SQL_ECHO", "false").lower() == "true",
)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    """Initialize database tables"""
    Base.metadata.create_all(bind=engine)


def get_db() -> Generator[Session, None, None]:
    """
    Dependency for FastAPI endpoints to get database session

    Usage:
        @app.get("/items")
        def read_items(db: Session = Depends(get_db)):
            return db.query(Item).all()
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_db_context():
    """
    Context manager for database sessions in non-FastAPI code

    Usage:
        with get_db_context() as db:
            query = db.query(QueryExecution).first()
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


class DatabaseManager:
    """Helper class for database operations"""

    @staticmethod
    def create_query_execution(
        db: Session, query_text: str, urgency: str, user_id: int = None
    ):
        """Create a new query execution record"""
        from .models import QueryExecution, QueryUrgencyEnum
        from hashlib import sha256

        query_hash = sha256(query_text.encode()).hexdigest()

        query_exec = QueryExecution(
            user_id=user_id,
            query_text=query_text,
            query_hash=query_hash,
            urgency=QueryUrgencyEnum[urgency.upper()],
        )
        db.add(query_exec)
        db.commit()
        db.refresh(query_exec)
        return query_exec

    @staticmethod
    def update_query_metrics(
        db: Session,
        query_id: int,
        execution_time: float,
        energy: float,
        carbon_intensity: float,
        emissions: float,
        plan: str,
        decision_reason: str = None,
    ):
        """Update query execution with metrics"""
        from .models import QueryExecution, ExecutionPlanEnum, QueryStatusEnum
        from datetime import datetime

        query = db.query(QueryExecution).filter(QueryExecution.id == query_id).first()
        if query:
            query.execution_time_ms = execution_time
            query.energy_joules = energy
            query.carbon_intensity_gco2_kwh = carbon_intensity
            query.estimated_emissions_gco2 = emissions
            query.selected_plan = ExecutionPlanEnum[plan.upper()]
            query.status = QueryStatusEnum.COMPLETED
            query.executed_at = datetime.utcnow()
            query.decision_reason = decision_reason
            db.commit()

    @staticmethod
    def store_carbon_data(
        db: Session,
        zone: str,
        timestamp,
        carbon_intensity: float,
        source: str = "electricitymaps",
    ):
        """Store carbon intensity data"""
        from .models import CarbonIntensityData

        data = CarbonIntensityData(
            zone=zone,
            timestamp=timestamp,
            carbon_intensity_gco2_kwh=carbon_intensity,
            data_source=source,
        )
        db.add(data)
        db.commit()

    @staticmethod
    def get_recent_carbon_data(db: Session, zone: str, hours: int = 24):
        """Get recent carbon intensity data"""
        from .models import CarbonIntensityData
        from datetime import datetime, timedelta

        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return (
            db.query(CarbonIntensityData)
            .filter(
                CarbonIntensityData.zone == zone,
                CarbonIntensityData.timestamp >= cutoff,
            )
            .order_by(CarbonIntensityData.timestamp.desc())
            .all()
        )

    @staticmethod
    def get_query_history(db: Session, user_id: int = None, limit: int = 100):
        """Get query execution history"""
        from .models import QueryExecution

        query = db.query(QueryExecution)
        if user_id:
            query = query.filter(QueryExecution.user_id == user_id)

        return query.order_by(QueryExecution.created_at.desc()).limit(limit).all()

    @staticmethod
    def get_emissions_summary(db: Session, days: int = 30):
        """Get emissions summary statistics"""
        from .models import QueryExecution
        from datetime import datetime, timedelta
        from sqlalchemy import func

        cutoff = datetime.utcnow() - timedelta(days=days)

        stats = (
            db.query(
                func.count(QueryExecution.id).label("total_queries"),
                func.sum(QueryExecution.estimated_emissions_gco2).label(
                    "total_emissions"
                ),
                func.avg(QueryExecution.estimated_emissions_gco2).label(
                    "avg_emissions"
                ),
                func.avg(QueryExecution.execution_time_ms).label("avg_execution_time"),
                func.avg(QueryExecution.carbon_intensity_gco2_kwh).label(
                    "avg_carbon_intensity"
                ),
            )
            .filter(QueryExecution.executed_at >= cutoff)
            .first()
        )

        return {
            "total_queries": stats.total_queries or 0,
            "total_emissions_gco2": float(stats.total_emissions or 0),
            "avg_emissions_gco2": float(stats.avg_emissions or 0),
            "avg_execution_time_ms": float(stats.avg_execution_time or 0),
            "avg_carbon_intensity": float(stats.avg_carbon_intensity or 0),
        }
