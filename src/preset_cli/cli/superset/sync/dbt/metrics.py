"""
Metric conversion.

This module is used to convert dbt metrics into Superset metrics.
"""

# pylint: disable=consider-using-f-string

from typing import Dict, List

from preset_cli.api.clients.dbt import FilterSchema, MetricSchema


def get_metric_expression(metric_name: str, metrics: Dict[str, MetricSchema]) -> str:
    """
    Return a SQL expression for a given dbt metric.
    """
    if metric_name not in metrics:
        raise Exception(f"Invalid metric {metric_name}")

    metric = metrics[metric_name]

    calculation_method = metric["calculation_method"]
    expression = metric["expression"]

    if metric.get("filters"):
        expression = apply_filters(expression, metric["filters"])

    simple_mappings = {
        "count": "COUNT",
        "sum": "SUM",
        "average": "AVG",
        "min": "MIN",
        "max": "MAX",
    }

    if calculation_method in simple_mappings:
        function = simple_mappings[calculation_method]
        return f"COALESCE({function}({expression}), 0)"

    if calculation_method == "count_distinct":
        return f"COUNT(DISTINCT {expression})"

    if calculation_method in ["derived", "expression"]:
        deps = [
            {"name": name, "expression": get_metric_expression(name, metrics)}
            for unnest_1 in metric["metrics"]
            for name in unnest_1
        ]
        for child in deps:
            expression = expression.replace(child["name"], child["expression"])
        return expression

    if calculation_method == "median":
        return f"COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {expression} ASC), 0)"

    sorted_metric = dict(sorted(metric.items()))
    raise Exception(f"Unable to generate metric expression from: {sorted_metric}")


def apply_filters(sql: str, filters: List[FilterSchema]) -> str:
    """
    Apply filters to SQL expression.
    """
    condition = " AND ".join(
        "{field} {operator} {value}".format(**filter_) for filter_ in filters
    )
    return f"CASE WHEN {condition} THEN {sql} END"
