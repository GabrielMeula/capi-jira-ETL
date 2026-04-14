import pytest
from capi_etl.transform.pulls import transform_pulls
from capi_etl.transform.commits import transform_commits


def test_pr_count(github_pulls):
    df = transform_pulls(github_pulls, repo="org/capivara-ai")
    assert len(df) == 2


def test_pr_id_format(github_pulls):
    df = transform_pulls(github_pulls, repo="org/capivara-ai")
    assert df["pr_id"].iloc[0] == "org/capivara-ai#101"


def test_issue_key_extracted(github_pulls):
    df = transform_pulls(github_pulls, repo="org/capivara-ai")
    pr101 = df[df["pr_id"] == "org/capivara-ai#101"]
    assert pr101["issue_key"].iloc[0] == "CAP-1"


def test_no_issue_key_when_absent(github_pulls):
    df = transform_pulls(github_pulls, repo="org/capivara-ai")
    pr202 = df[df["pr_id"] == "org/capivara-ai#202"]
    import pandas as pd
    assert pd.isna(pr202["issue_key"].iloc[0])


def test_time_to_merge(github_pulls):
    df = transform_pulls(github_pulls, repo="org/capivara-ai")
    pr101 = df[df["pr_id"] == "org/capivara-ai#101"]
    # 2026-01-10T09:00 → 2026-01-15T16:00 = 127h
    assert pr101["time_to_merge_horas"].iloc[0] == pytest.approx(127.0, abs=0.1)


def test_review_and_comment_counts(github_pulls):
    df = transform_pulls(github_pulls, repo="org/capivara-ai")
    pr101 = df[df["pr_id"] == "org/capivara-ai#101"]
    assert pr101["num_reviews"].iloc[0] == 2
    assert pr101["num_comments"].iloc[0] == 2


def test_commits_transform(github_pulls):
    df = transform_commits(github_pulls, repo="org/capivara-ai")
    assert len(df) == 1
    assert df["commit_hash"].iloc[0] == "abc123"
    assert df["pr_id"].iloc[0] == "org/capivara-ai#101"
