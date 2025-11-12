import sys
import os

# Add the absolute path to the 'src' directory to sys.path
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)

from utils.query_parser import QueryAnalyzer


def test_analyzer_smoke():
    a = QueryAnalyzer()
    out = a.analyze("SELECT 1")
    assert "complexity_score" in out
