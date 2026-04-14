"""Carrega e valida todas as variáveis de ambiente necessárias ao ETL."""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


def _require(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise EnvironmentError(f"Variável de ambiente obrigatória não definida: {name}")
    return value


@dataclass(frozen=True)
class Settings:
    jira_base_url: str
    jira_email: str
    jira_api_token: str
    jira_project_keys: list[str]
    jira_bug_project_name: str

    github_token: str
    github_repos: list[str]

    database_url: str
    etl_since_days: int
    log_level: str

    @classmethod
    def load(cls) -> "Settings":
        return cls(
            jira_base_url=_require("JIRA_BASE_URL").rstrip("/"),
            jira_email=_require("JIRA_EMAIL"),
            jira_api_token=_require("JIRA_API_TOKEN"),
            jira_project_keys=[k.strip() for k in _require("JIRA_PROJECT_KEYS").split(",")],
            jira_bug_project_name=_require("JIRA_BUG_PROJECT_NAME"),
            github_token=_require("GITHUB_TOKEN"),
            github_repos=[r.strip() for r in _require("GITHUB_REPOS").split(",")],
            database_url=_require("DATABASE_URL"),
            etl_since_days=int(os.getenv("ETL_SINCE_DAYS", "7")),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        )
