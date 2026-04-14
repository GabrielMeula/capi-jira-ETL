from capi_etl.transform.dim_projeto import transform_dim_projeto, _extract_adf_text
from capi_etl.transform.dim_tarefa import transform_dim_tarefa

_RAW_PROJECTS = [
    {
        "key": "CAP",
        "name": "Capivara AI",
        "description": {
            "type": "doc",
            "content": [
                {
                    "type": "paragraph",
                    "content": [{"type": "text", "text": "Produto principal de IA"}],
                }
            ],
        },
    },
    {"key": "BUG", "name": "Capi Bugs", "description": None},
]


def test_dim_projeto_rows(jira_issues):
    df = transform_dim_projeto(_RAW_PROJECTS)
    assert len(df) == 2
    assert set(df.columns) == {"project_key", "project_name", "project_description"}


def test_dim_projeto_description_extracted():
    df = transform_dim_projeto(_RAW_PROJECTS)
    cap = df[df["project_key"] == "CAP"]
    assert cap["project_description"].iloc[0] == "Produto principal de IA"


def test_dim_projeto_null_description():
    import pandas as pd
    df = transform_dim_projeto(_RAW_PROJECTS)
    bug = df[df["project_key"] == "BUG"]
    assert pd.isna(bug["project_description"].iloc[0])


def test_dim_projeto_empty():
    assert transform_dim_projeto([]).empty


def test_adf_text_nested():
    node = {
        "type": "doc",
        "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": "Hello"}]},
            {"type": "paragraph", "content": [{"type": "text", "text": "World"}]},
        ],
    }
    assert _extract_adf_text(node) == "Hello World"


# ── dim_tarefa ────────────────────────────────────────────────────────────────

def test_dim_tarefa_rows(jira_issues):
    df = transform_dim_tarefa(jira_issues)
    assert len(df) == 2
    assert set(df.columns) >= {"issue_key", "summary", "issue_type", "assignee_name", "reporter_name", "priority"}


def test_dim_tarefa_issue_key(jira_issues):
    df = transform_dim_tarefa(jira_issues)
    assert "CAP-1" in df["issue_key"].values
    assert "BUG-42" in df["issue_key"].values


def test_dim_tarefa_empty():
    assert transform_dim_tarefa([]).empty
