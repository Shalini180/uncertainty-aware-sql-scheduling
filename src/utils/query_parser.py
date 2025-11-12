import re
import sqlparse


class QueryAnalyzer:
    AGG_FUNCS = (" sum(", " avg(", " count(", " min(", " max(")

    def analyze(self, sql: str) -> dict:
        s = " " + sql.strip().lower() + " "

        has_join = " join " in s
        has_aggregation = any(f in s for f in self.AGG_FUNCS) or " group by " in s
        has_sort = " order by " in s
        has_subquery = bool(re.search(r"\(\s*select\b", s))

        tables = self._extract_tables(sql)

        # quick heuristic selectivity & complexity
        estimated_selectivity = self._estimate_selectivity(s)
        complexity_score = (
            1.0
            + 0.8 * has_join
            + 0.6 * has_aggregation
            + 0.3 * has_sort
            + 0.5 * has_subquery
            + 0.2 * max(0, len(tables) - 1)
        )

        return {
            "has_join": has_join,
            "has_aggregation": has_aggregation,
            "has_sort": has_sort,
            "has_subquery": has_subquery,
            "tables": tables,
            "estimated_selectivity": round(estimated_selectivity, 3),
            "complexity_score": round(complexity_score, 2),
        }

    def _extract_tables(self, sql: str):
        # simple FROM/JOIN capture (works for most cases)
        s = " " + sql.strip().lower() + " "
        names = re.findall(r"\bfrom\s+([a-z0-9_.]+)", s) + re.findall(
            r"\bjoin\s+([a-z0-9_.]+)", s
        )
        # strip aliases like "table t"
        clean = [n.split()[0] for n in names]
        return list(dict.fromkeys(clean))  # de-dup, keep order

    def _estimate_selectivity(self, s: str) -> float:
        # crude: if strong filters, assume low selectivity
        has_limit = " limit " in s
        equality_filters = len(re.findall(r"\b=\s*['\"\w]", s))
        range_filters = len(re.findall(r"\b>\s*|<\s*", s))
        score = (
            0.7
            - 0.1 * equality_filters
            - 0.05 * range_filters
            - (0.2 if has_limit else 0)
        )
        return min(0.95, max(0.05, score))
