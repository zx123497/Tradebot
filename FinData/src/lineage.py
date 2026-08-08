"""Minimal OpenLineage event emitter that posts to Marquez HTTP API."""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Any

import requests

PRODUCER_URI = "https://github.com/tradebot/findata"
SCHEMA_URL = (
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
)


class OpenLineageEmitter:
    def __init__(
        self,
        job_name: str,
        *,
        namespace: str | None = None,
        url: str | None = None,
        enabled: bool | None = None,
    ):
        self.job_name = job_name
        self.namespace = namespace or os.getenv("OPENLINEAGE_NAMESPACE", "tradebot")
        self.url = (url or os.getenv("OPENLINEAGE_URL", "http://localhost:5002")).rstrip(
            "/"
        )
        if enabled is None:
            enabled = os.getenv("OPENLINEAGE_ENABLED", "true").lower() in (
                "1",
                "true",
                "yes",
            )
        self.enabled = enabled
        self.run_id = str(uuid.uuid4())

    def start(
        self,
        inputs: list[dict[str, Any]] | None = None,
        outputs: list[dict[str, Any]] | None = None,
        run_facets: dict[str, Any] | None = None,
    ) -> None:
        self._emit("START", inputs or [], outputs or [], run_facets)

    def complete(
        self,
        inputs: list[dict[str, Any]] | None = None,
        outputs: list[dict[str, Any]] | None = None,
        run_facets: dict[str, Any] | None = None,
    ) -> None:
        self._emit("COMPLETE", inputs or [], outputs or [], run_facets)

    def fail(
        self,
        error: str | None = None,
        inputs: list[dict[str, Any]] | None = None,
        outputs: list[dict[str, Any]] | None = None,
    ) -> None:
        facets = {}
        if error:
            facets["errorMessage"] = {
                "_producer": PRODUCER_URI,
                "_schemaURL": (
                    "https://openlineage.io/spec/facets/1-0-0/ErrorMessageDatasetFacet.json"
                ),
                "message": error,
                "programmingLanguage": "PYTHON",
            }
        self._emit("FAIL", inputs or [], outputs or [], facets)

    def _emit(
        self,
        event_type: str,
        inputs: list[dict[str, Any]],
        outputs: list[dict[str, Any]],
        run_facets: dict[str, Any] | None,
    ) -> None:
        if not self.enabled:
            return

        event = {
            "eventType": event_type,
            "eventTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "run": {
                "runId": self.run_id,
                "facets": run_facets or {},
            },
            "job": {
                "namespace": self.namespace,
                "name": self.job_name,
                "facets": {},
            },
            "inputs": inputs,
            "outputs": outputs,
            "producer": PRODUCER_URI,
            "schemaURL": SCHEMA_URL,
        }

        try:
            response = requests.post(
                f"{self.url}/api/v1/lineage",
                json=event,
                timeout=5,
            )
            response.raise_for_status()
            print(f"[openlineage] {event_type} {self.namespace}.{self.job_name}")
        except Exception as exc:  # noqa: BLE001 - lineage must not break the pipeline
            print(f"[openlineage] failed to emit {event_type}: {exc}")


def kafka_dataset(topic: str, bootstrap: str = "localhost:9094") -> dict[str, Any]:
    return {
        "namespace": "kafka",
        "name": topic,
        "facets": {
            "dataSource": {
                "_producer": PRODUCER_URI,
                "_schemaURL": (
                    "https://openlineage.io/spec/facets/1-0-0/DataSourceDatasetFacet.json"
                ),
                "name": "kafka",
                "uri": f"kafka://{bootstrap}",
            }
        },
    }


def clickhouse_dataset(
    database: str,
    table: str,
    host: str = "localhost",
    port: int = 8123,
) -> dict[str, Any]:
    return {
        "namespace": "clickhouse",
        "name": f"{database}.{table}",
        "facets": {
            "dataSource": {
                "_producer": PRODUCER_URI,
                "_schemaURL": (
                    "https://openlineage.io/spec/facets/1-0-0/DataSourceDatasetFacet.json"
                ),
                "name": "clickhouse",
                "uri": f"clickhouse://{host}:{port}/{database}",
            }
        },
    }


def finnhub_dataset() -> dict[str, Any]:
    return {
        "namespace": "finnhub",
        "name": "trades",
        "facets": {
            "dataSource": {
                "_producer": PRODUCER_URI,
                "_schemaURL": (
                    "https://openlineage.io/spec/facets/1-0-0/DataSourceDatasetFacet.json"
                ),
                "name": "finnhub",
                "uri": "wss://ws.finnhub.io",
            }
        },
    }
