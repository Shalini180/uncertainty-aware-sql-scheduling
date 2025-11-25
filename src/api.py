# FastAPI application for Energy ML API

import os
from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from src.db.database import get_db, init_db
from src.db.models import (
    QueryExecution,
    QueryUrgencyEnum,
    QueryStatusEnum,
    QueryMetrics,
)
from src.core.engine import CarbonAwareQueryEngine
from src.optimizer.selector import QueryUrgency

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)

# Initialize FastAPI app
app = FastAPI(
    title="Carbon-Aware Query Engine API",
    description="API for executing SQL queries with carbon awareness",
    version="1.0.0",
)

# Add rate limiter to app state
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS middleware - restrictive for production
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "http://localhost:8501").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "X-API-Key"],
)

# Security - API Key Authentication
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: str = Depends(api_key_header)):
    """Verify API key from X-API-Key header"""
    expected_api_key = os.getenv("ENERGY_ML_API_KEY")
    if not expected_api_key:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="API key not configured on server",
        )
    if not api_key or api_key != expected_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    return api_key


# Pydantic models
class QueryRequest(BaseModel):
    query: str = Field(..., description="SQL query to execute")
    urgency: str = Field(
        default="medium", description="Query urgency: low, medium, high, critical"
    )
    explain: bool = Field(default=True, description="Include execution explanation")
    user_id: Optional[int] = None

    class Config:
        json_schema_extra = {
            "example": {
                "query": "SELECT COUNT(*) FROM users WHERE created_at > '2024-01-01'",
                "urgency": "medium",
                "explain": True,
            }
        }


class QueryResponse(BaseModel):
    query_id: int
    status: str
    execution_time_ms: Optional[float] = None
    energy_joules: Optional[float] = None
    carbon_intensity_gco2_kwh: Optional[float] = None
    estimated_emissions_gco2: Optional[float] = None
    selected_plan: Optional[str] = None
    decision_reason: Optional[str] = None
    deferred: bool = False
    scheduled_at: Optional[datetime] = None
    result: Optional[dict] = None
    # New uncertainty fields
    forecast_uncertainty_gco2_kwh: Optional[float] = None
    energy_std_dev_joules: Optional[float] = None


class QueryHistoryResponse(BaseModel):
    id: int
    query_text: str
    urgency: str
    status: str
    execution_time_ms: Optional[float]
    estimated_emissions_gco2: Optional[float]
    created_at: datetime
    executed_at: Optional[datetime]


class EmissionsSummaryResponse(BaseModel):
    total_queries: int
    total_emissions_gco2: float
    avg_emissions_gco2: float
    avg_execution_time_ms: float
    avg_carbon_intensity: float


class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    version: str
    database: str


# Endpoints
@app.get("/", response_model=dict)
async def root():
    """Root endpoint"""
    return {
        "message": "Carbon-Aware Query Engine API",
        "version": "1.0.0",
        "docs_url": "/docs",
    }


@app.get("/health", response_model=HealthResponse)
@limiter.limit("60/minute")
async def health_check(request: Request, db: Session = Depends(get_db)):
    """Health check endpoint with database connectivity check"""
    db_status = "disconnected"
    overall_status = "unhealthy"
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
        overall_status = "healthy"
        return HealthResponse(
            status=overall_status,
            timestamp=datetime.utcnow(),
            version="1.0.0",
            database=db_status,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database health check failed: {str(e)}",
        )


@app.post("/query/execute", response_model=QueryResponse)
@limiter.limit("5/minute")
async def execute_query(
    req: Request,
    request: QueryRequest,
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    """Execute a SQL query with carbon awareness"""
    try:
        # Create query execution record
        from src.db.database import DatabaseManager

        query_exec = DatabaseManager.create_query_execution(
            db=db,
            query_text=request.query,
            urgency=request.urgency,
            user_id=request.user_id,
        )
        # Initialize engine
        engine = CarbonAwareQueryEngine()
        # Map urgency string to enum
        urgency_map = {
            "low": QueryUrgency.LOW,
            "medium": QueryUrgency.MEDIUM,
            "high": QueryUrgency.HIGH,
            "critical": QueryUrgency.CRITICAL,
        }
        # Execute query
        result, metrics, decision = engine.execute_query(
            query=request.query,
            urgency=urgency_map.get(request.urgency.lower(), QueryUrgency.MEDIUM),
            explain=request.explain,
        )
        # Update metrics in DB
        DatabaseManager.update_query_metrics(
            db=db,
            query_id=query_exec.id,
            execution_time=metrics.get("execution_time_ms", 0),
            energy=metrics.get("energy_joules", 0),
            carbon_intensity=decision.carbon_intensity,
            emissions=metrics.get("estimated_emissions_gco2", 0),
            plan=decision.selected_plan,
            decision_reason=decision.reason,
        )
        # Retrieve uncertainty values from DB
        qe = db.query(QueryExecution).filter(QueryExecution.id == query_exec.id).first()
        qm = (
            db.query(QueryMetrics)
            .filter(QueryMetrics.query_id == query_exec.id)
            .first()
        )
        forecast_uncertainty = getattr(qe, "forecast_uncertainty_gco2_kwh", None)
        energy_std_dev = getattr(qm, "energy_std_dev_joules", None)
        # Check deferral
        deferred = decision.action == "defer"
        scheduled_at = None
        if deferred:
            scheduled_at = datetime.utcnow() + timedelta(minutes=decision.defer_minutes)
        return QueryResponse(
            query_id=query_exec.id,
            status="deferred" if deferred else "completed",
            execution_time_ms=metrics.get("execution_time_ms"),
            energy_joules=metrics.get("energy_joules"),
            carbon_intensity_gco2_kwh=decision.carbon_intensity,
            estimated_emissions_gco2=metrics.get("estimated_emissions_gco2"),
            selected_plan=decision.selected_plan,
            decision_reason=decision.reason,
            deferred=deferred,
            scheduled_at=scheduled_at,
            result=result if not deferred else None,
            forecast_uncertainty_gco2_kwh=forecast_uncertainty,
            energy_std_dev_joules=energy_std_dev,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Query execution failed: {str(e)}",
        )


@app.get("/query/history", response_model=List[QueryHistoryResponse])
@limiter.limit("30/minute")
async def get_query_history(
    request: Request,
    limit: int = 100,
    user_id: Optional[int] = None,
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    """Get query execution history"""
    from src.db.database import DatabaseManager

    queries = DatabaseManager.get_query_history(db=db, user_id=user_id, limit=limit)
    return [
        QueryHistoryResponse(
            id=q.id,
            query_text=q.query_text,
            urgency=q.urgency.value,
            status=q.status.value,
            execution_time_ms=q.execution_time_ms,
            estimated_emissions_gco2=q.estimated_emissions_gco2,
            created_at=q.created_at,
            executed_at=q.executed_at,
        )
        for q in queries
    ]


@app.get("/query/{query_id}", response_model=QueryHistoryResponse)
async def get_query_details(query_id: int, db: Session = Depends(get_db)):
    """Get details of a specific query execution"""
    query = db.query(QueryExecution).filter(QueryExecution.id == query_id).first()
    if not query:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Query with ID {query_id} not found",
        )
    return QueryHistoryResponse(
        id=query.id,
        query_text=query.query_text,
        urgency=query.urgency.value,
        status=query.status.value,
        execution_time_ms=query.execution_time_ms,
        estimated_emissions_gco2=query.estimated_emissions_gco2,
        created_at=query.created_at,
        executed_at=query.executed_at,
    )


@app.get("/metrics/uncertainty/{query_id}")
async def get_uncertainty_metrics(
    query_id: int, db: Session = Depends(get_db), api_key: str = Depends(verify_api_key)
):
    """Return forecast uncertainty and energy std dev for a given query execution"""
    qe = db.query(QueryExecution).filter(QueryExecution.id == query_id).first()
    qm = db.query(QueryMetrics).filter(QueryMetrics.query_id == query_id).first()
    if not qe or not qm:
        raise HTTPException(status_code=404, detail="Metrics not found for query_id")
    return {
        "forecast_uncertainty_gco2_kwh": getattr(
            qe, "forecast_uncertainty_gco2_kwh", None
        ),
        "energy_std_dev_joules": getattr(qm, "energy_std_dev_joules", None),
    }


@app.get("/emissions/summary", response_model=EmissionsSummaryResponse)
@limiter.limit("30/minute")
async def get_emissions_summary(
    request: Request,
    days: int = 30,
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    """Get emissions summary statistics"""
    from src.db.database import DatabaseManager

    summary = DatabaseManager.get_emissions_summary(db=db, days=days)
    return EmissionsSummaryResponse(**summary)


@app.get("/carbon/current")
async def get_current_carbon_intensity(zone: str = None):
    """Get current carbon intensity for a zone"""
    from src.optimizer.carbon_provider import CarbonProvider

    try:
        provider = CarbonProvider(zone=zone)
        intensity = provider.get_current_intensity()
        return {
            "zone": provider.zone,
            "carbon_intensity_gco2_kwh": intensity,
            "timestamp": datetime.utcnow(),
            "source": "electricitymaps" if provider.has_api_access else "fallback",
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get carbon intensity: {str(e)}",
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
