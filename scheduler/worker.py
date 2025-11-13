"""Background worker for scheduled query execution"""

import os
import time
from datetime import datetime, timedelta
from loguru import logger
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from ..db.database import get_db_context, DatabaseManager
from ..db.models import ScheduledQuery, QueryExecution, QueryStatusEnum
from ..core.engine import CarbonAwareQueryEngine
from ..optimizer.selector import QueryUrgency


class QuerySchedulerWorker:
    """Worker for executing scheduled queries"""

    def __init__(self):
        self.engine = CarbonAwareQueryEngine()
        self.scheduler = BlockingScheduler()
        self.setup_logging()

    def setup_logging(self):
        """Setup logging configuration"""
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)

        logger.add(
            f"{log_dir}/scheduler_{{time}}.log",
            rotation="1 day",
            retention="30 days",
            level="INFO",
        )

    def process_scheduled_queries(self):
        """Process queries scheduled for execution"""
        logger.info("Checking for scheduled queries...")

        with get_db_context() as db:
            # Get queries scheduled for now or earlier
            now = datetime.utcnow()

            scheduled = (
                db.query(ScheduledQuery)
                .filter(
                    ScheduledQuery.scheduled_for <= now,
                    ScheduledQuery.is_executed == False,
                    ScheduledQuery.execution_attempts < ScheduledQuery.max_attempts,
                )
                .all()
            )

            logger.info(f"Found {len(scheduled)} queries to execute")

            for scheduled_query in scheduled:
                try:
                    self.execute_scheduled_query(db, scheduled_query)
                except Exception as e:
                    logger.error(
                        f"Failed to execute scheduled query {scheduled_query.id}: {e}"
                    )
                    scheduled_query.execution_attempts += 1
                    db.commit()

    def execute_scheduled_query(self, db, scheduled_query: ScheduledQuery):
        """Execute a single scheduled query"""
        query_exec = (
            db.query(QueryExecution)
            .filter(QueryExecution.id == scheduled_query.query_execution_id)
            .first()
        )

        if not query_exec:
            logger.error(
                f"Query execution {scheduled_query.query_execution_id} not found"
            )
            return

        logger.info(
            f"Executing scheduled query {query_exec.id}: {query_exec.query_text[:50]}..."
        )

        try:
            # Update status to running
            query_exec.status = QueryStatusEnum.RUNNING
            db.commit()

            # Map urgency
            urgency_map = {
                "LOW": QueryUrgency.LOW,
                "MEDIUM": QueryUrgency.MEDIUM,
                "HIGH": QueryUrgency.HIGH,
                "CRITICAL": QueryUrgency.CRITICAL,
            }
            urgency = urgency_map.get(query_exec.urgency.name, QueryUrgency.MEDIUM)

            # Execute query
            result, metrics, decision = self.engine.execute_query(
                query=query_exec.query_text, urgency=urgency, explain=True
            )

            # Update query execution record
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

            # Mark scheduled query as executed
            scheduled_query.is_executed = True
            scheduled_query.execution_attempts += 1
            db.commit()

            logger.info(f"Successfully executed query {query_exec.id}")

        except Exception as e:
            logger.error(f"Error executing query {query_exec.id}: {e}")
            query_exec.status = QueryStatusEnum.FAILED
            scheduled_query.execution_attempts += 1
            db.commit()
            raise

    def cleanup_old_queries(self):
        """Clean up old executed scheduled queries"""
        logger.info("Cleaning up old scheduled queries...")

        with get_db_context() as db:
            # Delete queries executed more than 7 days ago
            cutoff = datetime.utcnow() - timedelta(days=7)

            deleted = (
                db.query(ScheduledQuery)
                .filter(
                    ScheduledQuery.is_executed == True,
                    ScheduledQuery.updated_at < cutoff,
                )
                .delete()
            )

            db.commit()
            logger.info(f"Deleted {deleted} old scheduled queries")

    def update_carbon_data(self):
        """Fetch and store current carbon intensity data"""
        logger.info("Updating carbon intensity data...")

        try:
            from ..optimizer.carbon_provider import CarbonProvider

            with get_db_context() as db:
                provider = CarbonProvider()
                intensity = provider.get_current_intensity()

                DatabaseManager.store_carbon_data(
                    db=db,
                    zone=provider.zone,
                    timestamp=datetime.utcnow(),
                    carbon_intensity=intensity,
                    source="electricitymaps" if provider.has_api_access else "fallback",
                )

                logger.info(f"Updated carbon intensity: {intensity} gCO2/kWh")

        except Exception as e:
            logger.error(f"Failed to update carbon data: {e}")

    def start(self):
        """Start the scheduler worker"""
        logger.info("Starting Query Scheduler Worker...")

        # Schedule tasks
        # Check for scheduled queries every minute
        self.scheduler.add_job(
            self.process_scheduled_queries,
            trigger=IntervalTrigger(minutes=1),
            id="process_scheduled_queries",
            name="Process scheduled queries",
            replace_existing=True,
        )

        # Update carbon data every 15 minutes
        self.scheduler.add_job(
            self.update_carbon_data,
            trigger=IntervalTrigger(minutes=15),
            id="update_carbon_data",
            name="Update carbon intensity data",
            replace_existing=True,
        )

        # Cleanup old queries daily
        self.scheduler.add_job(
            self.cleanup_old_queries,
            trigger=IntervalTrigger(hours=24),
            id="cleanup_old_queries",
            name="Cleanup old scheduled queries",
            replace_existing=True,
        )

        # Run initial carbon data update
        self.update_carbon_data()

        logger.info("Scheduler started successfully")

        # Start scheduler (blocking)
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler stopped")


if __name__ == "__main__":
    worker = QuerySchedulerWorker()
    worker.start()
