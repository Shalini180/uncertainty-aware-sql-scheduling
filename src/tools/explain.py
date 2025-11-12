import json, sys
from src.utils.query_parser import QueryAnalyzer
from src.core.compiler import MultiVariantCompiler

if __name__ == "__main__":
    sql = sys.argv[1] if len(sys.argv) > 1 else "SELECT 1"
    a = QueryAnalyzer().analyze(sql)
    v = MultiVariantCompiler().compile(sql)
    print("Analysis:", json.dumps(a, indent=2))
    print("\nVariants:")
    for k, x in v.items():
        print(
            f"- {k.value}: latency~{x.estimated_latency:.1f} ms, energy~{x.estimated_energy:.2f} J"
        )
