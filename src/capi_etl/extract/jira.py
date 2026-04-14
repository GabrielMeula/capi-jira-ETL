"""Extração de issues + changelog da API REST v3 do Jira."""
from __future__ import annotations

import logging
from base64 import b64encode
from typing import Any, Iterator

from capi_etl.config import Settings
from capi_etl.http import build_session

log = logging.getLogger(__name__)

_PAGE_SIZE = 100
_CHANGELOG_PAGE_SIZE = 100


def _auth_header(settings: Settings) -> str:
    token = b64encode(f"{settings.jira_email}:{settings.jira_api_token}".encode()).decode()
    return f"Basic {token}"


def search_issues(settings: Settings, jql: str) -> Iterator[dict[str, Any]]:
    """Itera sobre todas as issues via /search/jql (cursor-based) e busca changelog por issue."""
    session = build_session(
        headers={
            "Authorization": _auth_header(settings),
            "Accept": "application/json",
        }
    )
    # /rest/api/3/search foi descontinuado com paginação por startAt.
    # O endpoint atual usa cursor via nextPageToken.
    url = f"{settings.jira_base_url}/rest/api/3/search/jql"
    next_page_token: str | None = None
    total_yielded = 0

    while True:
        params: dict[str, Any] = {
            "jql": jql,
            "maxResults": _PAGE_SIZE,
            "fields": "summary,status,created,resolutiondate,project,issuetype,assignee,reporter,priority,description",
        }
        if next_page_token:
            params["nextPageToken"] = next_page_token

        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        issues = data.get("issues", [])
        log.debug("Jira search/jql: retornados=%d nextPageToken=%s", len(issues), data.get("nextPageToken"))

        for issue in issues:
            issue["changelog"] = {"histories": _fetch_changelog(session, settings, issue["key"])}
            yield issue

        total_yielded += len(issues)
        next_page_token = data.get("nextPageToken")
        if not next_page_token or not issues:
            break

    log.info("Jira: extração finalizada. Total de issues: %d", total_yielded)


def _fetch_changelog(
    session: Any,
    settings: Settings,
    issue_key: str,
) -> list[dict[str, Any]]:
    """Busca todas as páginas do changelog de uma issue."""
    url = f"{settings.jira_base_url}/rest/api/3/issue/{issue_key}/changelog"
    histories: list[dict[str, Any]] = []
    start_at = 0

    while True:
        resp = session.get(
            url,
            params={"startAt": start_at, "maxResults": _CHANGELOG_PAGE_SIZE},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        page = data.get("values", [])
        histories.extend(page)
        start_at += len(page)
        if len(page) < _CHANGELOG_PAGE_SIZE:
            break

    log.debug("Changelog %s: %d entradas.", issue_key, len(histories))
    return histories


def fetch_projects(settings: Settings) -> list[dict[str, Any]]:
    """Busca todos os projetos configurados via /rest/api/3/project/{key}."""
    session = build_session(
        headers={
            "Authorization": _auth_header(settings),
            "Accept": "application/json",
        }
    )
    projects = []
    for key in settings.jira_project_keys:
        url = f"{settings.jira_base_url}/rest/api/3/project/{key}"
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        projects.append(resp.json())
        log.debug("Projeto extraído: %s", key)
    log.info("Jira: %d projetos extraídos.", len(projects))
    return projects


def build_jql(settings: Settings, since_days: int | None = None) -> str:
    keys_csv = ", ".join(f'"{k}"' for k in settings.jira_project_keys)
    jql = f"project IN ({keys_csv}) ORDER BY updated DESC"
    if since_days is not None:
        jql = f'project IN ({keys_csv}) AND updated >= -{since_days}d ORDER BY updated DESC'
    return jql
