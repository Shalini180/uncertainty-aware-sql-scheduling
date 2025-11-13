"""Database models for energy ML project"""

from datetime import datetime
from typing import Optional
from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    DateTime,
    Boolean,
    JSON,
    ForeignKey,
    Text,
    Enum,
)
from sqlalchemy.orm import declarative_base, relationship
import enum

Base = declarative_base()


class QueryUrgencyEnum(enum.Enum):
    """Query urgency levels"""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ExecutionPlanEnum(enum.Enum):
    """Execution plan types"""

    FAST = "fast"
    BALANCED = "balanced"
    EFFICIENT = "efficient"


class QueryStatusEnum(enum.Enum):
    """Query execution status"""

    PENDING = "pending"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DEFERRED = "deferred"


class User(Base):
    """User model for authentication"""

    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), unique=True, index=True, nullable=False)
    username = Column(String(100), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    full_name = Column(String(255))
    is_active = Column(Boolean, default=True)
    is_superuser = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    queries = relationship("QueryExecution", back_populates="user")
    api_keys = relationship("APIKey", back_populates="user")


class APIKey(Base):
    """API keys for programmatic access"""

    __tablename__ = "api_keys"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    key = Column(String(255), unique=True, index=True, nullable=False)
    name = Column(String(100))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=True)
    last_used_at = Column(DateTime, nullable=True)

    # Relationships
    user = relationship("User", back_populates="api_keys")


class QueryExecution(Base):
    """Query execution records"""

    __tablename__ = "query_executions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    query_text = Column(Text, nullable=False)
    query_hash = Column(String(64), index=True)  # For deduplication
    urgency = Column(Enum(QueryUrgencyEnum), default=QueryUrgencyEnum.MEDIUM)
    status = Column(Enum(QueryStatusEnum), default=QueryStatusEnum.PENDING)

    # Execution details
    selected_plan = Column(Enum(ExecutionPlanEnum))
    execution_time_ms = Column(Float)
    energy_joules = Column(Float)
    carbon_intensity_gco2_kwh = Column(Float)
    estimated_emissions_gco2 = Column(Float)

    # Scheduling
    scheduled_at = Column(DateTime, nullable=True)
    executed_at = Column(DateTime, nullable=True)
    defer_duration_minutes = Column(Integer, nullable=True)

    # Decision reasoning
    decision_reason = Column(Text)
    explain_json = Column(JSON)

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    user = relationship("User", back_populates="queries")
    metrics = relationship("QueryMetrics", back_populates="query", uselist=False)


class QueryMetrics(Base):
    """Detailed query performance metrics"""

    __tablename__ = "query_metrics"

    id = Column(Integer, primary_key=True, index=True)
    query_id = Column(Integer, ForeignKey("query_executions.id"), unique=True)

    # Performance metrics
    cpu_percent = Column(Float)
    memory_mb = Column(Float)
    disk_io_mb = Column(Float)
    network_io_mb = Column(Float)

    # Query analysis
    num_joins = Column(Integer)
    num_aggregations = Column(Integer)
    estimated_rows = Column(Integer)
    actual_rows = Column(Integer)

    # Plan comparison
    fast_plan_time_ms = Column(Float)
    balanced_plan_time_ms = Column(Float)
    efficient_plan_time_ms = Column(Float)

    fast_plan_energy_j = Column(Float)
    balanced_plan_energy_j = Column(Float)
    efficient_plan_energy_j = Column(Float)

    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    query = relationship("QueryExecution", back_populates="metrics")


class CarbonIntensityData(Base):
    """Historical carbon intensity data"""

    __tablename__ = "carbon_intensity_data"

    id = Column(Integer, primary_key=True, index=True)
    zone = Column(String(50), index=True, nullable=False)
    timestamp = Column(DateTime, index=True, nullable=False)
    carbon_intensity_gco2_kwh = Column(Float, nullable=False)
    fossil_fuel_percentage = Column(Float)
    renewable_percentage = Column(Float)

    # Source tracking
    data_source = Column(String(100))  # e.g., 'electricitymaps', 'fallback_model'
    is_forecast = Column(Boolean, default=False)

    created_at = Column(DateTime, default=datetime.utcnow)

    class Config:
        indexes = [
            ("zone", "timestamp"),  # Composite index
        ]


class SystemMetrics(Base):
    """System-wide performance metrics"""

    __tablename__ = "system_metrics"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, index=True, default=datetime.utcnow)

    # System stats
    total_queries = Column(Integer, default=0)
    queries_deferred = Column(Integer, default=0)
    queries_failed = Column(Integer, default=0)

    # Carbon savings
    total_emissions_gco2 = Column(Float)
    emissions_saved_gco2 = Column(Float)
    avg_carbon_intensity = Column(Float)

    # Performance
    avg_execution_time_ms = Column(Float)
    avg_energy_per_query_j = Column(Float)

    # System health
    cpu_usage_percent = Column(Float)
    memory_usage_percent = Column(Float)
    disk_usage_percent = Column(Float)


class ScheduledQuery(Base):
    """Queries scheduled for future execution"""

    __tablename__ = "scheduled_queries"

    id = Column(Integer, primary_key=True, index=True)
    query_execution_id = Column(Integer, ForeignKey("query_executions.id"))

    scheduled_for = Column(DateTime, nullable=False, index=True)
    optimal_carbon_window_start = Column(DateTime)
    optimal_carbon_window_end = Column(DateTime)

    is_executed = Column(Boolean, default=False)
    execution_attempts = Column(Integer, default=0)
    max_attempts = Column(Integer, default=3)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
