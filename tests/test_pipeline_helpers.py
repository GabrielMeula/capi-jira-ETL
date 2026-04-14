import pandas as pd
from capi_etl.pipeline import _df_to_records


def test_converts_nat_to_none():
    df = pd.DataFrame([{"a": pd.NaT, "b": 1.0, "c": "x"}])
    rows = _df_to_records(df)
    assert rows[0]["a"] is None
    assert rows[0]["b"] == 1.0
    assert rows[0]["c"] == "x"


def test_converts_float_nan_to_none():
    df = pd.DataFrame([{"a": float("nan"), "b": 2}])
    rows = _df_to_records(df)
    assert rows[0]["a"] is None
    assert rows[0]["b"] == 2


def test_valid_timestamp_passes_through():
    ts = pd.Timestamp("2026-01-10 08:00:00", tz="America/Sao_Paulo")
    df = pd.DataFrame([{"ts": ts, "val": 42}])
    rows = _df_to_records(df)
    assert rows[0]["ts"] == ts
    assert rows[0]["val"] == 42


def test_none_stays_none():
    df = pd.DataFrame([{"a": None}])
    rows = _df_to_records(df)
    assert rows[0]["a"] is None


from capi_etl.transform.dim_user import build_dim_user


def test_build_dim_user_matched():
    df = build_dim_user(["Maria Silva"], ["dev-maria", "dev-joao"])
    matched = df[df["jira_display_name"] == "Maria Silva"]
    assert not matched.empty
    assert matched.iloc[0]["github_login"] == "dev-maria"


def test_build_dim_user_no_match_below_threshold():
    df = build_dim_user(["Xyz Abc"], ["totally-different-123"])
    row = df[df["jira_display_name"] == "Xyz Abc"].iloc[0]
    assert row["match_method"] == "unmatched"
    assert row["github_login"] is None


def test_build_dim_user_unmatched_github_appears():
    df = build_dim_user(["Maria Silva"], ["dev-maria", "bot-service"])
    gh_only = df[df["jira_display_name"].isna()]
    assert "bot-service" in gh_only["github_login"].values
