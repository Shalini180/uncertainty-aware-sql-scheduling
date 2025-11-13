"""FastAPI application for Energy ML API"""

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from ..db.database import get_db, init_db
from ..db.models import QueryExecution, QueryUrgencyEnum, QueryStatusEnum
from ..core.engine import CarbonAwareQueryEngine
from ..optimizer.selector import QueryUrgency

# Initialize FastAPI app
app = FastAPI(
    title="Carbon-Aware Query Engine API",
    description="API for executing SQL queries with carbon awareness",
    version="1.0.0",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Initialize database on startup
@app.on_event("startup")
async def startup_event():
    init_db()


# Security
security = HTTPBearer()


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
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy", timestamp=datetime.utcnow(), version="1.0.0"
    )


@app.post("/query/execute", response_model=QueryResponse)
async def execute_query(request: QueryRequest, db: Session = Depends(get_db)):
    """
    Execute a SQL query with carbon awareness

    - **query**: SQL query to execute
    - **urgency**: Query urgency level (low, medium, high, critical)
    - **explain**: Whether to include execution explanation
    """
    try:
        # Create query execution record
        from ..db.database import DatabaseManager

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

        # Update database with results
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

        # Check if query was deferred
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
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Query execution failed: {str(e)}",
        )


@app.get("/query/history", response_model=List[QueryHistoryResponse])
async def get_query_history(
    limit: int = 100, user_id: Optional[int] = None, db: Session = Depends(get_db)
):
    """Get query execution history"""
    from ..db.database import DatabaseManager

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


@app.get("/emissions/summary", response_model=EmissionsSummaryResponse)
async def get_emissions_summary(days: int = 30, db: Session = Depends(get_db)):
    """Get emissions summary statistics"""
    from ..db.database import DatabaseManager

    summary = DatabaseManager.get_emissions_summary(db=db, days=days)

    return EmissionsSummaryResponse(**summary)


@app.get("/carbon/current")
async def get_current_carbon_intensity(zone: str = None):
    """Get current carbon intensity for a zone"""
    from ..optimizer.carbon_provider import CarbonProvider

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
