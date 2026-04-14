"""DDL das 4 tabelas de fato via SQLAlchemy Core."""
from __future__ import annotations

from sqlalchemy import (
    Boolean,
    Column,
    Integer,
    MetaData,
    Numeric,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import TIMESTAMP

metadata = MetaData()

dim_projeto = Table(
    "dim_projeto",
    metadata,
    Column("project_key", Text, primary_key=True),
    Column("project_name", Text, nullable=False),
    Column("project_description", Text, nullable=True),
)

dim_tarefa = Table(
    "dim_tarefa",
    metadata,
    Column("issue_key", Text, primary_key=True),
    Column("summary", Text, nullable=True),
    Column("description", Text, nullable=True),
    Column("issue_type", Text, nullable=True),
    Column("assignee_name", Text, nullable=True),
    Column("reporter_name", Text, nullable=True),
    Column("priority", Text, nullable=True),
)

fato_issues = Table(
    "fato_issues",
    metadata,
    Column("issue_key", Text, primary_key=True),
    Column("project_name", Text, nullable=False, index=True),
    Column("is_bug", Boolean, nullable=False, default=False),
    Column("created_date", TIMESTAMP(timezone=True), nullable=False),
    Column("resolved_date", TIMESTAMP(timezone=True), nullable=True),
    Column("lead_time_dias", Numeric, nullable=True),
    Column("status_atual", Text, nullable=False),
)

fato_changelog = Table(
    "fato_changelog",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("issue_key", Text, nullable=False, index=True),
    Column("status_anterior", Text, nullable=True),
    Column("novo_status", Text, nullable=False),
    Column("data_mudanca", TIMESTAMP(timezone=True), nullable=False),
    Column("tempo_no_status_horas", Numeric, nullable=True),
    UniqueConstraint("issue_key", "data_mudanca", "novo_status", name="uq_changelog"),
)

fato_pull_requests = Table(
    "fato_pull_requests",
    metadata,
    Column("pr_id", Text, primary_key=True),   # formato: "org/repo#123"
    Column("issue_key", Text, nullable=True, index=True),
    Column("repo_name", Text, nullable=False),
    Column("author_name", Text, nullable=True),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
    Column("merged_at", TIMESTAMP(timezone=True), nullable=True),
    Column("time_to_merge_horas", Numeric, nullable=True),
    Column("num_comments", Integer, nullable=False, default=0),
    Column("num_reviews", Integer, nullable=False, default=0),
)

fato_commits = Table(
    "fato_commits",
    metadata,
    Column("commit_hash", Text, primary_key=True),
    Column("pr_id", Text, nullable=True, index=True),
    Column("author_name", Text, nullable=True),
    Column("commit_date", TIMESTAMP(timezone=True), nullable=True),
)
