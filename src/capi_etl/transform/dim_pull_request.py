"""Transforma PRs enriquecidos em linhas para dim_pull_request."""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)


def transform_dim_pull_request(raw_pulls: list[dict[str, Any]], repo: str) -> pd.DataFrame:
    if not raw_pulls:
        return pd.DataFrame()

    rows = []
    for pr in raw_pulls:
        rows.append(
            {
                "pr_id": f"{repo}#{pr['number']}",
                "title": pr.get("title"),
                "head_branch": (pr.get("head") or {}).get("ref"),
                "base_branch": (pr.get("base") or {}).get("ref"),
                "state": pr.get("state"),
            }
        )

    df = pd.DataFrame(rows)
    log.info("transform_dim_pull_request [%s]: %d linhas geradas.", repo, len(df))
    return df
