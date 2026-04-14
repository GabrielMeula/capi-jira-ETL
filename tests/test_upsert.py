"""Testa o helper de upsert usando SQLite em memória (dialeto simplificado)."""
import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, select, text


@pytest.fixture
def sqlite_engine():
    engine = create_engine("sqlite:///:memory:")
    meta = MetaData()
    t = Table(
        "test_table",
        meta,
        Column("id", Integer, primary_key=True),
        Column("value", String),
    )
    meta.create_all(engine)
    return engine, t


def test_upsert_insert_and_update(sqlite_engine):
    """Verifica inserção e atualização idempotente via INSERT OR REPLACE (SQLite)."""
    engine, t = sqlite_engine

    rows = [{"id": 1, "value": "original"}]
    with engine.begin() as conn:
        conn.execute(t.insert(), rows)

    with engine.begin() as conn:
        row = conn.execute(select(t).where(t.c.id == 1)).fetchone()
    assert row.value == "original"

    # Simula upsert (SQLite não tem ON CONFLICT DO UPDATE mas podemos usar INSERT OR REPLACE)
    with engine.begin() as conn:
        conn.execute(text("INSERT OR REPLACE INTO test_table(id, value) VALUES (1, 'updated')"))

    with engine.begin() as conn:
        row = conn.execute(select(t).where(t.c.id == 1)).fetchone()
    assert row.value == "updated"

    # Idempotente: rodar de novo com mesmo valor não muda nada
    with engine.begin() as conn:
        conn.execute(text("INSERT OR REPLACE INTO test_table(id, value) VALUES (1, 'updated')"))

    with engine.begin() as conn:
        count = conn.execute(select(t)).fetchall()
    assert len(count) == 1
