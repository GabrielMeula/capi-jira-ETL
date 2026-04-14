import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def jira_issues():
    return json.loads((FIXTURES / "jira_issues.json").read_text())


@pytest.fixture
def github_pulls():
    return json.loads((FIXTURES / "github_pulls.json").read_text())
