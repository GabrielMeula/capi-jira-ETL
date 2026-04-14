"""Transforma o changelog das issues em linhas para fato_changelog."""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)

_TZ = "America/Sao_Paulo"


def transform_changelog(raw_issues: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []

    for issue in raw_issues:
        issue_key = issue["key"]
        histories = issue.get("changelog", {}).get("histories", [])

        for entry in histories:
            created_raw = entry.get("created")
            for item in entry.get("items", []):
                if item.get("field") != "status":
                    continue
                rows.append(
                    {
                        "issue_key": issue_key,
                        "status_anterior": item.get("fromString"),
                        "novo_status": item.get("toString", ""),
                        "data_mudanca": pd.to_datetime(created_raw, utc=True).tz_convert(_TZ)
                        if created_raw
                        else pd.NaT,
                    }
                )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df.sort_values(["issue_key", "data_mudanca"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Calcula tempo_no_status_horas: diferença entre esta mudança e a próxima
    df["proxima_mudanca"] = df.groupby("issue_key")["data_mudanca"].shift(-1)
    df["tempo_no_status_horas"] = (
        (df["proxima_mudanca"] - df["data_mudanca"]).dt.total_seconds() / 3600
    )
    df.drop(columns=["proxima_mudanca"], inplace=True)

    log.info("transform_changelog: %d linhas geradas.", len(df))
    return df
