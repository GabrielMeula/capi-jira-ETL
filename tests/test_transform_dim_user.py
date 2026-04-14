import pytest
from capi_etl.transform.dim_user import build_dim_user


# Dados de entrada que simulam o que o pipeline passa
JIRA_NAMES = ["Maria Silva", "João Ferreira", "Carlos Dev"]
GITHUB_LOGINS = ["dev-maria", "dev-joao", "carlos-dev", "unknown-bot"]


def test_all_jira_names_present():
    df = build_dim_user(JIRA_NAMES, GITHUB_LOGINS)
    assert set(df["jira_display_name"].dropna()) == set(JIRA_NAMES)


def test_unmatched_github_logins_included():
    df = build_dim_user(JIRA_NAMES, GITHUB_LOGINS)
    # "unknown-bot" não casou com nenhum nome Jira
    unmatched_gh = df[df["jira_display_name"].isna()]
    assert "unknown-bot" in unmatched_gh["github_login"].values


def test_fuzzy_match_maria():
    df = build_dim_user(["Maria Silva"], ["dev-maria"])
    row = df[df["jira_display_name"] == "Maria Silva"].iloc[0]
    assert row["github_login"] == "dev-maria"
    assert row["match_method"] == "fuzzy"
    assert row["match_score"] >= 0.6


def test_exact_match():
    df = build_dim_user(["devmaria"], ["devmaria"])
    row = df[df["jira_display_name"] == "devmaria"].iloc[0]
    assert row["match_method"] == "exact"
    assert float(row["match_score"]) == pytest.approx(1.0)


def test_unmatched_jira_name():
    df = build_dim_user(["Pessoa Inexistente"], ["totally-different"])
    row = df[df["jira_display_name"] == "Pessoa Inexistente"].iloc[0]
    assert row["match_method"] == "unmatched"
    assert row["github_login"] is None


def test_user_key_uniqueness():
    df = build_dim_user(JIRA_NAMES, GITHUB_LOGINS)
    assert df["user_key"].nunique() == len(df)


def test_empty_inputs():
    df = build_dim_user([], [])
    assert df.empty


def test_only_github_logins():
    df = build_dim_user([], ["bot-user"])
    assert len(df) == 1
    assert df.iloc[0]["github_login"] == "bot-user"
    assert df.iloc[0]["jira_display_name"] is None
