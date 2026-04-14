"""Entrypoint CLI do ETL. Invocado via `capi-etl` ou `python -m capi_etl`."""
from __future__ import annotations

import argparse
import sys


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="capi-etl",
        description="Pipeline ETL: Jira + GitHub → PostgreSQL",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="incremental",
        help="'full' extrai tudo; 'incremental' usa ETL_SINCE_DAYS (padrão: incremental).",
    )
    parser.add_argument(
        "--only",
        choices=["jira", "github", "all"],
        default="all",
        help="Executa apenas o domínio especificado (padrão: all).",
    )
    args = parser.parse_args()

    # Importações tardias para que erros de config falhem somente ao rodar
    from capi_etl.config import Settings
    from capi_etl.logging_setup import setup_logging
    from capi_etl.pipeline import run

    try:
        settings = Settings.load()
    except EnvironmentError as exc:
        print(f"[ERRO DE CONFIGURAÇÃO] {exc}", file=sys.stderr)
        sys.exit(2)

    setup_logging(settings.log_level)

    try:
        run(settings, mode=args.mode, only=args.only)
    except Exception as exc:
        import logging
        logging.getLogger(__name__).exception("Pipeline encerrado com erro: %s", exc)
        sys.exit(1)
