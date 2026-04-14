"""Constrói dim_user emparelhando nomes do Jira com logins do GitHub por similaridade."""
from __future__ import annotations

import logging
import re
from difflib import SequenceMatcher
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)

# 0.6 empiricamente validado para nomes PT-BR vs logins GitHub estilo "dev-<nome>"
_THRESHOLD = 0.6
_NON_ALNUM = re.compile(r"[^a-z0-9]")


def _slug(name: str) -> str:
    """Normaliza para comparação: lowercase, sem caracteres especiais."""
    return _NON_ALNUM.sub("", name.lower())


def _similarity(a: str, b: str) -> float:
    """Retorna a similaridade máxima entre a string inteira e cada token de 'a'."""
    full = SequenceMatcher(None, _slug(a), _slug(b)).ratio()
    tokens = a.split()
    if len(tokens) > 1:
        token_best = max(
            SequenceMatcher(None, _slug(t), _slug(b)).ratio() for t in tokens
        )
        return max(full, token_best)
    return full


def build_dim_user(
    jira_names: list[str],
    github_logins: list[str],
) -> pd.DataFrame:
    """
    Retorna DataFrame com colunas:
      user_key, jira_display_name, github_login, match_score, match_method
    """
    if not jira_names and not github_logins:
        return pd.DataFrame()

    jira_names = list(dict.fromkeys(jira_names))

    rows: list[dict[str, Any]] = []
    matched_logins: set[str] = set()

    for jira_name in jira_names:
        best_login: str | None = None
        best_score = 0.0

        for login in github_logins:
            if login in matched_logins:
                continue
            score = _similarity(jira_name, login)
            if score > best_score:
                best_score = score
                best_login = login

        if best_score >= _THRESHOLD and best_login is not None:
            method = "exact" if best_score >= 1.0 else "fuzzy"
            matched_logins.add(best_login)
            user_key = f"jira:{_slug(jira_name)}"
            rows.append(
                {
                    "user_key": user_key,
                    "jira_display_name": jira_name,
                    "github_login": best_login,
                    "match_score": round(best_score, 4),
                    "match_method": method,
                }
            )
        else:
            user_key = f"jira:{_slug(jira_name)}"
            rows.append(
                {
                    "user_key": user_key,
                    "jira_display_name": jira_name,
                    "github_login": None,
                    "match_score": None,
                    "match_method": "unmatched",
                }
            )

    # Logins GitHub sem par Jira
    for login in github_logins:
        if not _slug(login):
            log.warning("Skipping GitHub login with empty slug: %r", login)
            continue
        if login not in matched_logins:
            rows.append(
                {
                    "user_key": f"gh:{_slug(login)}",
                    "jira_display_name": None,
                    "github_login": login,
                    "match_score": None,
                    "match_method": "unmatched",
                }
            )

    df = pd.DataFrame(rows, dtype=object)
    log.info("build_dim_user: %d usuários mapeados (%d Jira, %d GitHub).",
             len(df), len(jira_names), len(github_logins))
    return df
