"""Transforma commits de PRs em linhas para fato_commits."""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)

_TZ = "America/Sao_Paulo"


def transform_commits(raw_pulls: list[dict[str, Any]], repo: str) -> pd.DataFrame:
    if not raw_pulls:
        return pd.DataFrame()

    rows = []
    for pr in raw_pulls:
        pr_id = f"{repo}#{pr['number']}"
        for commit in pr.get("_commits", []):
            sha = commit.get("sha", "")
            committer = commit.get("commit", {}).get("committer", {})
            author = commit.get("commit", {}).get("author", {})
            date_raw = committer.get("date") or author.get("date")
            author_name = (
                (commit.get("author") or {}).get("login")
                or author.get("name")
            )

            rows.append(
                {
                    "commit_hash": sha,
                    "pr_id": pr_id,
                    "author_name": author_name,
                    "commit_date": pd.to_datetime(date_raw, utc=True).tz_convert(_TZ)
                    if date_raw
                    else None,
                }
            )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).drop_duplicates(subset=["commit_hash"])
    log.info("transform_commits [%s]: %d linhas geradas.", repo, len(df))
    return df
