"""Orquestra as etapas extract → transform → load para cada domínio."""
from __future__ import annotations

import logging

import pandas as pd

from capi_etl.config import Settings
from capi_etl.extract.jira import build_jql, fetch_projects, search_issues
from capi_etl.extract.github import enrich_pulls, list_pulls
from capi_etl.load.db import create_all_tables, get_connection
from capi_etl.load.schema import (
    dim_projeto,
    dim_tarefa,
    fato_changelog,
    fato_commits,
    fato_issues,
    fato_pull_requests,
)
from capi_etl.load.upsert import upsert
from capi_etl.transform.changelog import transform_changelog
from capi_etl.transform.commits import transform_commits
from capi_etl.transform.dim_projeto import transform_dim_projeto
from capi_etl.transform.dim_tarefa import transform_dim_tarefa
from capi_etl.transform.issues import transform_issues
from capi_etl.transform.pulls import transform_pulls

log = logging.getLogger(__name__)


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    """Converte DataFrame em lista de dicts, convertendo NaT/NaN/None → None."""
    def _clean(v):
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass
        return v

    return [
        {k: _clean(v) for k, v in row.items()}
        for row in df.to_dict(orient="records")
    ]


def run_jira(settings: Settings, since_days: int | None) -> None:
    log.info("=== ETL Jira — início ===")

    # Dimensão projetos
    raw_projects = fetch_projects(settings)
    df_dim_projeto = transform_dim_projeto(raw_projects)

    # Issues + changelog
    jql = build_jql(settings, since_days)
    log.info("JQL: %s", jql)
    raw_issues = list(search_issues(settings, jql))
    log.info("Issues extraídas: %d", len(raw_issues))

    df_issues = transform_issues(raw_issues, settings.jira_bug_project_name)
    df_changelog = transform_changelog(raw_issues)
    df_dim_tarefa = transform_dim_tarefa(raw_issues)

    with get_connection(settings.database_url) as conn:
        upsert(conn, dim_projeto, _df_to_records(df_dim_projeto), ["project_key"])
        upsert(conn, dim_tarefa, _df_to_records(df_dim_tarefa), ["issue_key"])
        upsert(conn, fato_issues, _df_to_records(df_issues), ["issue_key"])
        if not df_changelog.empty:
            upsert(conn, fato_changelog, _df_to_records(df_changelog), ["issue_key", "data_mudanca", "novo_status"])

    log.info("=== ETL Jira — fim ===")


def run_github(settings: Settings, since_days: int | None) -> None:
    log.info("=== ETL GitHub — início ===")

    all_pulls_rows: list[dict] = []
    all_commits_rows: list[dict] = []

    for repo in settings.github_repos:
        log.info("GitHub: processando repo %s", repo)
        pulls = list_pulls(settings, repo, since_days)
        enriched = enrich_pulls(settings, repo, pulls)

        df_pulls = transform_pulls(enriched, repo)
        df_commits = transform_commits(enriched, repo)

        if not df_pulls.empty:
            all_pulls_rows.extend(_df_to_records(df_pulls))
        if not df_commits.empty:
            all_commits_rows.extend(_df_to_records(df_commits))

    with get_connection(settings.database_url) as conn:
        upsert(conn, fato_pull_requests, all_pulls_rows, ["pr_id"])
        upsert(conn, fato_commits, all_commits_rows, ["commit_hash"])

    log.info("=== ETL GitHub — fim ===")


def run(settings: Settings, mode: str, only: str) -> None:
    since_days = settings.etl_since_days if mode == "incremental" else None

    log.info("Pipeline iniciado — mode=%s only=%s since_days=%s", mode, only, since_days)
    create_all_tables(settings.database_url)

    if only in ("jira", "all"):
        run_jira(settings, since_days)

    if only in ("github", "all"):
        run_github(settings, since_days)

    log.info("Pipeline concluído.")
