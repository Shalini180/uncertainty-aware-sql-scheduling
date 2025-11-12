class QueryAnalyzer:
    def analyze(self, sql: str) -> dict:
        return {
            "has_join": False,
            "has_aggregation": False,
            "has_sort": False,
            "has_subquery": False,
            "tables": [],
            "estimated_selectivity": 0.5,
            "complexity_score": 1.0,
        }
