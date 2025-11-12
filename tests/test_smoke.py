from src.utils.query_parser import QueryAnalyzer


def test_analyzer_smoke():
    a = QueryAnalyzer()
    out = a.analyze("SELECT 1")
    assert "complexity_score" in out
