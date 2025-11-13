from typing import Dict
import sqlparse


class QueryAnalyzer:
    def analyze(self, sql: str) -> Dict:
        parsed = sqlparse.parse(sql or "")
        upper = (sql or "").upper()
        return {
            "has_join": " JOIN " in upper,
            "has_aggregation": any(
                k in upper for k in [" SUM(", " COUNT(", " AVG(", " GROUP BY "]
            ),
            "has_sort": " ORDER BY " in upper,
            "has_subquery": "SELECT" in upper[upper.find("SELECT") + 6 :].upper(),
            "tables": [],  # keep simple for now
            "estimated_selectivity": 0.5,
            "complexity_score": 50.0,
        }
