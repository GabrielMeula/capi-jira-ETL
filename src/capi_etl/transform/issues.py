"""Transforma issues brutas do Jira em linhas para fato_issues."""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)

_TZ = "America/Sao_Paulo"


def transform_issues(raw_issues: list[dict[str, Any]], bug_project_name: str) -> pd.DataFrame:
    if not raw_issues:
        return pd.DataFrame()

    rows = []
    for issue in raw_issues:
        fields = issue.get("fields", {})
        project_name = (fields.get("project") or {}).get("name", "")
        created_raw = fields.get("created")
        resolved_raw = fields.get("resolutiondate")
        status = (fields.get("status") or {}).get("name", "")

        created = pd.to_datetime(created_raw, utc=True).tz_convert(_TZ) if created_raw else pd.NaT
        resolved = pd.to_datetime(resolved_raw, utc=True).tz_convert(_TZ) if resolved_raw else pd.NaT

        is_bug = project_name == bug_project_name

        lead_time = None
        if pd.notna(resolved) and pd.notna(created):
            lead_time = (resolved - created).total_seconds() / 86400  # dias (float)

        rows.append(
            {
                "issue_key": issue["key"],
                "project_name": project_name,
                "is_bug": is_bug,
                "created_date": created,
                "resolved_date": resolved if pd.notna(resolved) else None,
                "lead_time_dias": lead_time,
                "status_atual": status,
            }
        )

    df = pd.DataFrame(rows)
    log.info("transform_issues: %d linhas geradas.", len(df))
    return df
