"""
Metric conversion.

This module is used to convert dbt metrics into Superset metrics.
"""

# pylint: disable=consider-using-f-string

from functools import partial
from typing import Dict, List

from jinja2 import Template

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
        return f"{function}({expression})"

    if calculation_method == "count_distinct":
        return f"COUNT(DISTINCT {expression})"

    if calculation_method in ["derived", "expression"]:
        template = Template(expression)
        return template.render(metric=partial(get_metric_expression, metrics=metrics))

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
