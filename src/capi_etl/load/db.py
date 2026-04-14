"""Engine SQLAlchemy e context manager de conexão."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, Connection
from sqlalchemy.engine import Engine

_engine: Engine | None = None


def get_engine(database_url: str) -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(database_url, pool_pre_ping=True)
    return _engine


@contextmanager
def get_connection(database_url: str) -> Generator[Connection, None, None]:
    engine = get_engine(database_url)
    with engine.begin() as conn:
        yield conn


def create_all_tables(database_url: str) -> None:
    from capi_etl.load.schema import metadata
    engine = get_engine(database_url)
    metadata.create_all(engine)
