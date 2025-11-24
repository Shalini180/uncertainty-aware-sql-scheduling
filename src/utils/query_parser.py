from typing import Dict
import sqlparse


class QueryAnalyzer:
    def analyze(self, sql: str) -> Dict:
        parsed = sqlparse.parse(sql or "")
        upper = (sql or "").upper()

        # Extract table names from FROM and JOIN clauses
        tables = []
        tokens = upper.split()
        for i, token in enumerate(tokens):
            if token in ("FROM", "JOIN") and i + 1 < len(tokens):
                # Get next token and clean it
                next_token = tokens[i + 1]
                # Remove common SQL keywords and punctuation
                table = next_token.strip(",()").split()[0]
                # Handle aliases (e.g., "orders o" -> "orders")
                if table and table not in (
                    "SELECT",
                    "WHERE",
                    "ON",
                    "INNER",
                    "LEFT",
                    "RIGHT",
                    "OUTER",
                ):
                    tables.append(table.lower())

        return {
            "has_join": " JOIN " in upper,
            "has_aggregation": any(
                k in upper for k in [" SUM(", " COUNT(", " AVG(", " GROUP BY "]
            ),
            "has_sort": " ORDER BY " in upper,
            "has_subquery": "SELECT" in upper[upper.find("SELECT") + 6 :].upper(),
            "tables": list(set(tables)),  # Remove duplicates
            "estimated_selectivity": 0.5,
            "complexity_score": 50.0,
        }
