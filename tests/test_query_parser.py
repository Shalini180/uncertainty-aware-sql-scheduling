from src.utils.query_parser import QueryAnalyzer


def test_analyze_flags_and_tables():
    a = QueryAnalyzer()
    sql = "SELECT COUNT(*) FROM orders o JOIN customers c ON o.cid=c.id WHERE o.amount>100 GROUP BY c.segment ORDER BY c.segment"
    out = a.analyze(sql)
    assert out["has_join"] and out["has_aggregation"] and out["has_sort"]
    assert "orders" in out["tables"] and "customers" in out["tables"]
    assert 0.05 <= out["estimated_selectivity"] <= 0.95
    assert out["complexity_score"] >= 1.0


def test_subquery_detection():
    a = QueryAnalyzer()
    sql = "SELECT * FROM t WHERE id IN (SELECT id FROM u)"
    out = a.analyze(sql)
    assert out["has_subquery"]
