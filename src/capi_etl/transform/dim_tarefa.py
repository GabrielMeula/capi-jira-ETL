"""Transforma campos descritivos das issues em linhas para dim_tarefa."""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)


def _extract_adf_text(node: Any) -> str:
    """Extrai texto plano de um nó Atlassian Document Format (ADF)."""
    if not node or not isinstance(node, dict):
        return ""
    if node.get("type") == "text":
        return node.get("text", "")
    parts = [_extract_adf_text(child) for child in node.get("content", [])]
    return " ".join(p for p in parts if p).strip()


def transform_dim_tarefa(raw_issues: list[dict[str, Any]]) -> pd.DataFrame:
    if not raw_issues:
        return pd.DataFrame()

    rows = []
    for issue in raw_issues:
        fields = issue.get("fields", {})

        description_raw = fields.get("description")
        description = _extract_adf_text(description_raw) if isinstance(description_raw, dict) else None

        rows.append(
            {
                "issue_key": issue["key"],
                "summary": fields.get("summary"),
                "description": description or None,
                "issue_type": (fields.get("issuetype") or {}).get("name"),
                "assignee_name": (fields.get("assignee") or {}).get("displayName"),
                "reporter_name": (fields.get("reporter") or {}).get("displayName"),
                "priority": (fields.get("priority") or {}).get("name"),
            }
        )

    df = pd.DataFrame(rows)
    log.info("transform_dim_tarefa: %d linhas geradas.", len(df))
    return df
