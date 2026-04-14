"""Transforma PRs enriquecidos em linhas para fato_pull_requests."""
from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)

_TZ = "America/Sao_Paulo"
_JIRA_KEY_RE = re.compile(r"\b([A-Z][A-Z0-9]+-\d+)\b")


def _extract_issue_key(pr: dict[str, Any]) -> str | None:
    # Tenta título primeiro, depois nome do branch
    for text in (pr.get("title", ""), (pr.get("head") or {}).get("ref", "")):
        m = _JIRA_KEY_RE.search(text or "")
        if m:
            return m.group(1)
    return None


def transform_pulls(raw_pulls: list[dict[str, Any]], repo: str) -> pd.DataFrame:
    if not raw_pulls:
        return pd.DataFrame()

    rows = []
    for pr in raw_pulls:
        created_raw = pr.get("created_at")
        merged_raw = pr.get("merged_at")

        created = pd.to_datetime(created_raw, utc=True).tz_convert(_TZ) if created_raw else pd.NaT
        merged = pd.to_datetime(merged_raw, utc=True).tz_convert(_TZ) if merged_raw else pd.NaT

        time_to_merge = None
        if pd.notna(merged) and pd.notna(created):
            time_to_merge = (merged - created).total_seconds() / 3600

        rows.append(
            {
                "pr_id": f"{repo}#{pr['number']}",
                "issue_key": _extract_issue_key(pr),
                "repo_name": repo,
                "author_name": (pr.get("user") or {}).get("login"),
                "created_at": created,
                "merged_at": merged if pd.notna(merged) else None,
                "time_to_merge_horas": time_to_merge,
                "num_comments": len(pr.get("_comments", [])),
                "num_reviews": len(pr.get("_reviews", [])),
            }
        )

    df = pd.DataFrame(rows)
    log.info("transform_pulls [%s]: %d linhas geradas.", repo, len(df))
    return df
