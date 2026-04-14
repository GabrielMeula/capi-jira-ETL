"""Transforma dados de projeto do Jira em linhas para dim_projeto."""
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


def transform_dim_projeto(raw_projects: list[dict[str, Any]]) -> pd.DataFrame:
    if not raw_projects:
        return pd.DataFrame()

    rows = []
    for proj in raw_projects:
        description_raw = proj.get("description")
        description = _extract_adf_text(description_raw) if isinstance(description_raw, dict) else (description_raw or None)

        rows.append(
            {
                "project_key": proj.get("key", ""),
                "project_name": proj.get("name", ""),
                "project_description": description or None,
            }
        )

    df = pd.DataFrame(rows)
    log.info("transform_dim_projeto: %d linhas geradas.", len(df))
    return df
