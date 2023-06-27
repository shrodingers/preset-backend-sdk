"""
Sync dbt datasets/etrics to Superset.
"""

# pylint: disable=consider-using-f-string

import json
import logging
from typing import Any, Dict, List

from sqlalchemy.engine import create_engine
from sqlalchemy.engine.url import URL as SQLAlchemyURL
from sqlalchemy.engine.url import make_url
from yarl import URL

from preset_cli.api.clients.dbt import MetricSchema, ModelSchema
from preset_cli.api.clients.superset import SupersetClient
from preset_cli.api.operators import OneToMany
from preset_cli.cli.superset.sync.dbt.metrics import get_metric_expression

_logger = logging.getLogger(__name__)


def model_in_database(model: ModelSchema, url: SQLAlchemyURL) -> bool:
    """
    Return if a model is in the same database as a SQLAlchemy URI.
    """
    if url.drivername == "bigquery":
        return model["database"] == url.host
    return model["database"] == url.database


def create_dataset(
    client: SupersetClient,
    database: Dict[str, Any],
    model: ModelSchema,
) -> Dict[str, Any]:
    """
    Create a physical or virtual dataset.

    Virtual datasets are created when the table database is different from the main
    database, for systems that support cross-database queries (Trino, BigQuery, etc.)
    """
    url = make_url(database["sqlalchemy_uri"])
    if model_in_database(model, url):
        kwargs = {
            "database": database["id"],
            "schema": model["schema"],
            "table_name": model.get("alias") or model["name"],
        }
    else:
        engine = create_engine(url)
        quote = engine.dialect.identifier_preparer.quote
        source = ".".join(quote(model[key]) for key in ("database", "schema", "alias"))
        kwargs = {
            "database": database["id"],
            "schema": model["schema"],
            "table_name": model.get("alias") or model["name"],
            "sql": f"SELECT * FROM {source}",
        }

    return client.create_dataset(**kwargs)


def sync_datasets(  # pylint: disable=too-many-locals, too-many-branches, too-many-arguments
    client: SupersetClient,
    models: List[ModelSchema],
    metrics: List[MetricSchema],
    database: Any,
    disallow_edits: bool,
    external_url_prefix: str,
) -> List[Any]:
    """
    Read the dbt manifest and import models as datasets with metrics.
    """
    base_url = URL(external_url_prefix) if external_url_prefix else None

    # add datasets
    datasets = []
    for model in models:
        filters = {
            "database": OneToMany(database["id"]),
            "table_name": model.get("alias") or model["name"],
        }
        existing = client.get_datasets(**filters)
        if len(existing) > 1:
            unique_id = model["unique_id"]
            existing = [
                item
                for item in existing
                if unique_id == json.loads(item["extra"])["unique_id"]
            ]
        if len(existing) > 1:
            raise Exception("More than one dataset found")

        if existing:
            dataset = existing[0]
            _logger.info("Updating dataset %s", model["unique_id"])
        else:
            _logger.info("Creating dataset %s", model["unique_id"])
            try:
                dataset = create_dataset(client, database, model)
            except Exception:  # pylint: disable=broad-except
                _logger.exception("Unable to create dataset")
                continue

        extra = {
            "unique_id": model["unique_id"],
            "depends_on": "ref('{name}')".format(**model),
            "certification": {
                "details": "This table is produced by dbt",
            },
        }

        dataset_info = client.get_dataset(dataset["id"])
        existing_metrics = dataset_info["metrics"]
        metric_keys = [
            "d3format",
            "description",
            "expression",
            "extra",
            "metric_name",
            "metric_type",
            "verbose_name",
            "warning_text",
        ]

        def get_deps_metrics(metric):
            metrics_len = len(metric["metrics"]) or 0
            if metrics_len > 0:
                result = [
                    get_deps_metrics(
                        [
                            metric_value
                            for metric_value in metrics
                            if metric_value["name"] == real_metric
                        ][0]
                    )
                    for sub_metric in metric.get("metrics")
                    for real_metric in sub_metric
                ]
                return metric.get("depends_on") + [
                    elem for all in result for elem in all
                ]
            return metric.get("depends_on")

        model_metrics = {
            metric["name"]: metric
            for metric in metrics
            if model["unique_id"] in get_deps_metrics(metric)
        }
        model_metrics_names = [dbt_metric["name"] for dbt_metric in metrics]
        dataset_metrics = (
            [
                {key: value for key, value in metric.items() if key in metric_keys}
                for metric in existing_metrics
                if metric["metric_name"] != "count"
                and metric["metric_name"] not in model_metrics_names
            ]
            if existing_metrics
            else []
        )
        for name, metric in model_metrics.items():
            meta = metric.get("meta", {})
            kwargs = meta.pop("superset", {})
            dataset_metrics.append(
                {
                    "expression": get_metric_expression(name, model_metrics),
                    "metric_name": name,
                    "metric_type": metric.get("type")  # dbt < 1.3
                    or metric.get("calculation_method"),  # dbt >= 1.3
                    "verbose_name": metric.get("label", name),
                    "description": metric.get("description", ""),
                    "extra": json.dumps(meta),
                    **kwargs,
                },
            )

        # update dataset clearing metrics...
        update = {
            "description": model.get("description", ""),
            "schema": model["schema"],
            "extra": json.dumps(extra),
            "is_managed_externally": disallow_edits,
            "metrics": [],
        }
        update.update(model.get("meta", {}).get("superset", {}))
        if base_url:
            fragment = "!/model/{unique_id}".format(**model)
            update["external_url"] = str(base_url.with_fragment(fragment))
        client.update_dataset(dataset["id"], override_columns=True, **update)

        # ...then update metrics
        if dataset_metrics:
            update = {
                "metrics": dataset_metrics,
            }
            client.update_dataset(dataset["id"], override_columns=False, **update)

        # update column descriptions
        update = {
            "columns": [
                {
                    "column_name": name,
                    "description": column.get("description", ""),
                    "is_dttm": column["data_type"] == "timestamp"
                    if not column.get("meta", {})
                    .get("superset", {})
                    .get("is_dttm", False)
                    else False,
                }
                for name, column in model.get("columns", {}).items()
            ],
        }
        if update["columns"]:
            client.update_dataset(dataset["id"], override_columns=True, **update)

        datasets.append(dataset)

    return datasets
