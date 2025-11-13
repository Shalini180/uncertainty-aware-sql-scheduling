import duckdb
from pathlib import Path


class TPCHBenchmark:
    def __init__(self, scale_factor: float = 0.01):
        self.sf = scale_factor
        self.data_dir = Path("data/tpch")
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def generate_data(self):
        con = duckdb.connect()
        con.execute(f"CALL dbgen(sf={self.sf})")
        for t in [
            "customer",
            "lineitem",
            "nation",
            "orders",
            "part",
            "partsupp",
            "region",
            "supplier",
        ]:
            con.execute(f"COPY {t} TO '{self.data_dir}/{t}.parquet' (FORMAT PARQUET)")
        con.close()

    def load_data(self, conn: duckdb.DuckDBPyConnection):
        for t in [
            "customer",
            "lineitem",
            "nation",
            "orders",
            "part",
            "partsupp",
            "region",
            "supplier",
        ]:
            p = self.data_dir / f"{t}.parquet"
            if not p.exists():
                raise FileNotFoundError("Run generate_data first")
            conn.execute(f"CREATE OR REPLACE TABLE {t} AS SELECT * FROM '{p}'")

    def get_queries(self) -> dict:
        return {
            "Q1": "SELECT COUNT(*) FROM lineitem",
            "Q3": "SELECT o_orderpriority, COUNT(*) FROM orders GROUP BY o_orderpriority",
            "Q6": "SELECT SUM(l_extendedprice*l_discount) FROM lineitem WHERE l_discount BETWEEN 0.05 AND 0.07",
            "Q12": "SELECT l_shipmode, COUNT(*) FROM lineitem GROUP BY l_shipmode",
        }
