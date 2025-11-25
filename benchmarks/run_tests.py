import sys
import os
import json
import random
import time
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv

# Load environment variables FIRST
load_dotenv()

# Configure database URL for benchmarks BEFORE importing database modules
if "BENCHMARK_DB_URL" in os.environ:
    os.environ["DATABASE_URL"] = os.environ["BENCHMARK_DB_URL"]

# Add project root to path
sys.path.append(os.getcwd())

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

if "BENCHMARK_DB_URL" in os.environ:
    logger.info(f"Using benchmark database: {os.environ['BENCHMARK_DB_URL']}")

# NOW import database modules (after DATABASE_URL is set)
from sqlalchemy.orm import Session
from src.db.database import init_db, get_db_context, DatabaseManager, engine
from src.core.compiler import MultiVariantCompiler, ExecutionStrategy
from src.core.executor import QueryExecutor
from src.optimizer.carbon_api import CarbonIntensity

# --- 1. Data Generation ---


def generate_carbon_history(file_path: str = "data/carbon_history.json"):
    """Generate 7 days of synthetic carbon intensity data"""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    history = []
    start_time = datetime.now() - timedelta(days=7)

    for i in range(7 * 24):  # 7 days * 24 hours
        current_time = start_time + timedelta(hours=i)
        hour = current_time.hour

        # Simulate daily curve
        if 10 <= hour <= 16:  # Solar peak (low carbon)
            base = 200
            noise = random.randint(-20, 20)
        elif 17 <= hour <= 21:  # Evening peak (high carbon)
            base = 500
            noise = random.randint(-50, 50)
        else:  # Night/Morning (medium)
            base = 350
            noise = random.randint(-30, 30)

        intensity = base + noise

        history.append(
            {
                "timestamp": current_time.isoformat(),
                "zone": "US-CAL-CISO",
                "carbon_intensity": intensity,
                "source": "synthetic",
            }
        )

    with open(file_path, "w") as f:
        json.dump(history, f, indent=2)

    logger.info(f"Generated carbon history at {file_path}")
    return history


# --- 2. Mock Data Setup ---


def setup_mock_db():
    """Initialize DuckDB with mock data"""
    # We use the executor's connection logic or just create a local duckdb for this test
    pass


# --- 3. Query Suite ---

QUERIES = [
    # 1. Simple Select
    "SELECT * FROM users LIMIT 100",
    # 2. Filtering
    "SELECT * FROM products WHERE category = 'Electronics'",
    # 3. Aggregation
    "SELECT category, COUNT(*) FROM products GROUP BY category",
    # 4. Join
    "SELECT u.username, o.id FROM users u JOIN orders o ON u.id = o.user_id",
    # 5. Complex Filtering
    "SELECT * FROM orders WHERE total_amount > 100 AND status = 'completed'",
    # 6. Date Range
    "SELECT * FROM orders WHERE created_at > '2023-01-01'",
    # 7. Multi-Join
    "SELECT u.username, p.name, o.total_amount FROM users u JOIN orders o ON u.id = o.user_id JOIN order_items oi ON o.id = oi.order_id JOIN products p ON oi.product_id = p.id",
    # 8. Group By with Having
    "SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id HAVING count(*) > 5",
    # 9. Order By
    "SELECT * FROM products ORDER BY price DESC LIMIT 10",
    # 10. Distinct
    "SELECT DISTINCT status FROM orders",
    # 11. Subquery
    "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total_amount > 500)",
    # 12. CTE
    "WITH high_value_orders AS (SELECT * FROM orders WHERE total_amount > 200) SELECT COUNT(*) FROM high_value_orders",
    # 13. Window Function
    "SELECT id, total_amount, RANK() OVER (ORDER BY total_amount DESC) as rank FROM orders",
    # 14. Union
    "SELECT id FROM users WHERE is_active = true UNION SELECT user_id FROM orders",
    # 15. Case Statement
    "SELECT id, CASE WHEN total_amount > 100 THEN 'High' ELSE 'Low' END as value_class FROM orders",
]

# --- 4. Benchmarking Loop ---


def run_benchmarks():
    logger.info("Starting Benchmarks...")

    # Initialize Metadata DB (Postgres/SQLite)
    init_db()

    # Generate Carbon Data
    generate_carbon_history(os.path.join(os.getcwd(), "data", "carbon_history.json"))

    # Setup Compiler and Executor
    compiler = MultiVariantCompiler()
    executor = QueryExecutor()

    # Initialize Profiler
    from src.core.profiler import EnergyProfiler

    profiler = EnergyProfiler()

    # We need to populate the DuckDB instance.
    def populate_data(conn):
        # Create Tables
        conn.execute(
            "CREATE TABLE IF NOT EXISTS users (id INTEGER, username VARCHAR, is_active BOOLEAN)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS products (id INTEGER, name VARCHAR, category VARCHAR, price DECIMAL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS orders (id INTEGER, user_id INTEGER, total_amount DECIMAL, status VARCHAR, created_at TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS order_items (order_id INTEGER, product_id INTEGER)"
        )

        # Insert Mock Data (Small dataset)
        # Users
        conn.execute(
            f"INSERT INTO users SELECT i, 'user_' || i, i % 2 = 0 FROM range(1000) t(i)"
        )
        # Products
        conn.execute(
            f"INSERT INTO products SELECT i, 'product_' || i, CASE WHEN i % 3 = 0 THEN 'Electronics' ELSE 'Books' END, i * 10 FROM range(500) t(i)"
        )
        # Orders
        conn.execute(
            f"INSERT INTO orders SELECT i, i % 1000, i * 50, CASE WHEN i % 2 = 0 THEN 'completed' ELSE 'pending' END, '2023-01-01'::TIMESTAMP + INTERVAL (i) DAY FROM range(5000) t(i)"
        )
        # Order Items
        conn.execute(
            f"INSERT INTO order_items SELECT i % 5000, i % 500 FROM range(10000) t(i)"
        )

    # Strategies to test
    strategies = {
        "Strategy A (Latency-First)": ExecutionStrategy.FAST,
        "Strategy B (Carbon-Aware)": ExecutionStrategy.BALANCED,
        "Strategy C (Balanced Hybrid)": ExecutionStrategy.EFFICIENT,
    }

    # Create default user if not exists
    from src.db.models import User

    with get_db_context() as db:
        user = db.query(User).filter(User.id == 1).first()
        if not user:
            user = User(
                id=1,
                email="benchmark@example.com",
                username="benchmark_user",
                hashed_password="hashed_password",
                full_name="Benchmark User",
            )
            db.add(user)
            db.commit()
            logger.info("Created benchmark user")

    with get_db_context() as db:
        for q_idx, sql in enumerate(QUERIES):
            logger.info(f"Running Query {q_idx + 1}/{len(QUERIES)}")

            # Compile Variants
            variants = compiler.compile(sql)

            for strat_name, strat_enum in strategies.items():
                variant = variants[strat_enum]

                conn = compiler.get_connection(variant)
                populate_data(conn)

                # Execute with uncertainty profiling
                # We'll profile the execution function
                def execute_query():
                    # Use the optimized SQL
                    query_to_run = variant.sql or sql
                    return conn.execute(query_to_run).fetchall()

                try:
                    # Run 5 iterations to get uncertainty
                    result_data, metrics, energy_std_dev = (
                        profiler.profile_with_uncertainty(execute_query, iterations=5)
                    )
                    success = True

                    # Use profiled metrics
                    duration_ms = metrics.duration_ms
                    energy_j = metrics.energy_joules

                except Exception as e:
                    logger.error(f"Query failed: {e}")
                    success = False
                    duration_ms = 0
                    energy_j = 0
                    energy_std_dev = 0

                # Metrics
                carbon_intensity = 300  # Mock or get from history
                emissions_g = (energy_j / 3.6e6) * carbon_intensity

                # Get forecast uncertainty (mock for benchmark)
                forecast_uncertainty = 15.0  # Mock value from CarbonAPI logic

                # Log to DB
                if success:
                    # Create Execution Record
                    query_exec = DatabaseManager.create_query_execution(
                        db=db,
                        query_text=sql,
                        urgency="medium",  # Default
                        user_id=1,  # Mock user
                    )

                    # Update Metrics
                    DatabaseManager.update_query_metrics(
                        db=db,
                        query_id=query_exec.id,
                        execution_time=duration_ms,
                        energy=energy_j,
                        carbon_intensity=carbon_intensity,
                        emissions=emissions_g,
                        plan=strat_enum.value,
                        decision_reason=f"Benchmark: {strat_name}",
                    )

                    # Update QueryExecution for forecast uncertainty
                    from src.db.models import QueryExecution, QueryMetrics

                    db.query(QueryExecution).filter(
                        QueryExecution.id == query_exec.id
                    ).update({"forecast_uncertainty_gco2_kwh": forecast_uncertainty})

                    # Update QueryMetrics for energy std dev
                    metric_record = (
                        db.query(QueryMetrics)
                        .filter(QueryMetrics.query_id == query_exec.id)
                        .first()
                    )
                    if not metric_record:
                        metric_record = QueryMetrics(query_id=query_exec.id)
                        db.add(metric_record)

                    metric_record.energy_std_dev_joules = energy_std_dev

                    db.commit()

                    logger.info(
                        f"  {strat_name}: {duration_ms:.2f}ms, {emissions_g:.4f}gCO2, σ={energy_std_dev:.2f}J"
                    )

    logger.info("Benchmarks Completed.")


if __name__ == "__main__":
    run_benchmarks()
