from capi_etl.transform.dim_pull_request import transform_dim_pull_request


def test_row_count(github_pulls):
    df = transform_dim_pull_request(github_pulls, repo="org/capivara-ai")
    assert len(df) == 2


def test_pr_id_format(github_pulls):
    df = transform_dim_pull_request(github_pulls, repo="org/capivara-ai")
    assert "org/capivara-ai#101" in df["pr_id"].values


def test_columns_present(github_pulls):
    df = transform_dim_pull_request(github_pulls, repo="org/capivara-ai")
    assert set(df.columns) >= {"pr_id", "title", "head_branch", "base_branch", "state"}


def test_title_and_branches(github_pulls):
    df = transform_dim_pull_request(github_pulls, repo="org/capivara-ai")
    row = df[df["pr_id"] == "org/capivara-ai#101"].iloc[0]
    assert row["title"] == "feat: [CAP-1] implement login flow"
    assert row["head_branch"] == "feature/CAP-1-login"
    assert row["base_branch"] == "main"
    assert row["state"] == "closed"


def test_open_state_pr(github_pulls):
    df = transform_dim_pull_request(github_pulls, repo="org/capivara-ai")
    row = df[df["pr_id"] == "org/capivara-ai#202"].iloc[0]
    assert row["state"] == "open"


def test_null_head_and_base_produces_none():
    pr = {"number": 999, "title": "orphan", "state": "open", "head": None, "base": None}
    df = transform_dim_pull_request([pr], repo="org/repo")
    row = df.iloc[0]
    assert row["head_branch"] is None
    assert row["base_branch"] is None


def test_empty_input():
    df = transform_dim_pull_request([], repo="org/capivara-ai")
    assert df.empty
