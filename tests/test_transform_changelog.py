from capi_etl.transform.changelog import transform_changelog


def test_rows_count(jira_issues):
    df = transform_changelog(jira_issues)
    # CAP-1 tem 2 transições, BUG-42 tem 1 → 3 linhas
    assert len(df) == 3


def test_columns(jira_issues):
    df = transform_changelog(jira_issues)
    assert set(df.columns) >= {"issue_key", "status_anterior", "novo_status", "data_mudanca", "tempo_no_status_horas"}


def test_last_row_tempo_null(jira_issues):
    df = transform_changelog(jira_issues)
    # Última transição de cada issue deve ter tempo_no_status_horas NaN
    for key in df["issue_key"].unique():
        last = df[df["issue_key"] == key].iloc[-1]
        assert last["tempo_no_status_horas"] != last["tempo_no_status_horas"]  # NaN check


def test_tempo_no_status_horas(jira_issues):
    df = transform_changelog(jira_issues)
    cap1 = df[df["issue_key"] == "CAP-1"].reset_index(drop=True)
    # Primeira transição: 2026-01-11 → 2026-01-15: ~104 horas
    assert cap1.loc[0, "tempo_no_status_horas"] == pytest.approx(104, abs=1)


def test_empty_input():
    df = transform_changelog([])
    assert df.empty


import pytest
