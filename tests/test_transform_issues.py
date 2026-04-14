import pytest
from capi_etl.transform.issues import transform_issues


def test_basic_fields(jira_issues):
    df = transform_issues(jira_issues, bug_project_name="Capi Bugs")
    assert len(df) == 2
    assert set(df.columns) >= {"issue_key", "project_name", "is_bug", "lead_time_dias", "status_atual"}


def test_is_bug_flag(jira_issues):
    df = transform_issues(jira_issues, bug_project_name="Capi Bugs")
    assert not df.loc[df["issue_key"] == "CAP-1", "is_bug"].iloc[0]
    assert df.loc[df["issue_key"] == "BUG-42", "is_bug"].iloc[0]


def test_lead_time(jira_issues):
    df = transform_issues(jira_issues, bug_project_name="Capi Bugs")
    cap1 = df.loc[df["issue_key"] == "CAP-1", "lead_time_dias"].iloc[0]
    assert cap1 == pytest.approx(5.375, abs=0.01)  # ~5 dias + 9 h


def test_empty_input():
    df = transform_issues([], bug_project_name="Capi Bugs")
    assert df.empty
