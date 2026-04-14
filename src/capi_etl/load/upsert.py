"""Helper genérico de upsert via INSERT ... ON CONFLICT DO UPDATE."""
from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import Connection, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

log = logging.getLogger(__name__)


def upsert(
    conn: Connection,
    table: Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
) -> int:
    """Faz upsert em lote na tabela indicada. Retorna rowcount estimado."""
    if not rows:
        log.debug("upsert: nenhuma linha para %s, pulando.", table.name)
        return 0

    stmt = pg_insert(table).values(rows)

    # Colunas a atualizar = todas exceto as de conflito
    update_cols = {
        c.name: stmt.excluded[c.name]
        for c in table.columns
        if c.name not in conflict_cols
    }

    if update_cols:
        stmt = stmt.on_conflict_do_update(
            index_elements=conflict_cols,
            set_=update_cols,
        )
    else:
        stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)

    result = conn.execute(stmt)
    log.info("upsert %s: %d linha(s) processada(s).", table.name, len(rows))
    return len(rows)
