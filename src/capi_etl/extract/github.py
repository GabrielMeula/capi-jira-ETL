"""Extração de PRs, reviews, comentários e commits da API REST v3 do GitHub."""
from __future__ import annotations

import logging
import re
from typing import Any, Iterator

from capi_etl.config import Settings
from capi_etl.http import build_session

log = logging.getLogger(__name__)

_PAGE_SIZE = 100
_LINK_RE = re.compile(r'<([^>]+)>;\s*rel="next"')


def _session(settings: Settings):
    return build_session(
        headers={
            "Authorization": f"Bearer {settings.github_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    )


def _paginate(session: Any, url: str, params: dict | None = None) -> Iterator[dict[str, Any]]:
    """Itera sobre todas as páginas de um endpoint GitHub usando o header Link."""
    next_url: str | None = url
    base_params = dict(params or {})
    base_params.setdefault("per_page", _PAGE_SIZE)

    while next_url:
        resp = session.get(next_url, params=base_params if next_url == url else None, timeout=30)
        resp.raise_for_status()
        items = resp.json()
        if isinstance(items, list):
            yield from items
        link_header = resp.headers.get("Link", "")
        m = _LINK_RE.search(link_header)
        next_url = m.group(1) if m else None


def list_pulls(settings: Settings, repo: str, since_days: int | None = None) -> list[dict[str, Any]]:
    """Retorna todos os PRs do repo (state=all). Filtra por updated_at se since_days fornecido."""
    session = _session(settings)
    url = f"https://api.github.com/repos/{repo}/pulls"
    pulls = list(_paginate(session, url, {"state": "all", "sort": "updated", "direction": "desc"}))

    if since_days is not None:
        import datetime, timezone as _tz
        cutoff = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(days=since_days)
        )
        pulls = [
            p for p in pulls
            if p.get("updated_at") and
               datetime.datetime.fromisoformat(p["updated_at"].replace("Z", "+00:00")) >= cutoff
        ]

    log.info("GitHub %s: %d PRs extraídos.", repo, len(pulls))
    return pulls


def list_pr_reviews(session: Any, repo: str, pr_number: int) -> list[dict[str, Any]]:
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/reviews"
    return list(_paginate(session, url))


def list_pr_comments(session: Any, repo: str, pr_number: int) -> list[dict[str, Any]]:
    url = f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments"
    return list(_paginate(session, url))


def list_pr_commits(session: Any, repo: str, pr_number: int) -> list[dict[str, Any]]:
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/commits"
    return list(_paginate(session, url))


def enrich_pulls(settings: Settings, repo: str, pulls: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Adiciona reviews, comentários e commits a cada PR."""
    session = _session(settings)
    enriched = []
    for pr in pulls:
        n = pr["number"]
        pr["_reviews"] = list_pr_reviews(session, repo, n)
        pr["_comments"] = list_pr_comments(session, repo, n)
        pr["_commits"] = list_pr_commits(session, repo, n)
        enriched.append(pr)
    return enriched
