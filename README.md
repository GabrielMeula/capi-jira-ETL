# capi-etl — Pipeline ETL: Jira + GitHub → PostgreSQL

Pipeline de dados em Python que extrai issues/changelog do Jira e PRs/commits/reviews do GitHub, transforma com pandas e carrega via upsert em PostgreSQL, alimentando dashboards de qualidade e velocidade no BI.

## Pré-requisitos

- Python ≥ 3.11
- [uv](https://docs.astral.sh/uv/) instalado
- PostgreSQL acessível

## Setup local

```bash
# Clone o repositório e entre na pasta
git clone <repo-url> && cd capi-jira-intetgration

# Instale as dependências
uv sync

# Copie e preencha as variáveis de ambiente
cp .env.example .env
# Edite .env com suas credenciais (veja seção Configuração abaixo)
```

## Configuração (variáveis de ambiente)

| Variável | Obrigatória | Descrição |
|---|---|---|
| `JIRA_BASE_URL` | sim | URL base do Jira (ex: `https://minha-org.atlassian.net`) |
| `JIRA_EMAIL` | sim | E-mail da conta Jira |
| `JIRA_API_TOKEN` | sim | API Token gerado em id.atlassian.com |
| `JIRA_PROJECT_KEYS` | sim | Chaves dos projetos separadas por vírgula (ex: `CAP,PLAT,BUG`) |
| `JIRA_BUG_PROJECT_NAME` | sim | Nome exato do projeto de bugs (ex: `Capi Bugs`) |
| `GITHUB_TOKEN` | sim | Personal Access Token com permissão de leitura |
| `GITHUB_REPOS` | sim | Repos no formato `org/repo`, separados por vírgula |
| `DATABASE_URL` | sim | SQLAlchemy URL (ex: `postgresql+psycopg2://user:pwd@host:5432/bi`) |
| `ETL_SINCE_DAYS` | não | Janela de lookback incremental em dias (padrão: `7`) |
| `LOG_LEVEL` | não | `DEBUG`, `INFO`, `WARNING`, `ERROR` (padrão: `INFO`) |

## Executando o pipeline

```bash
# Carga completa (toda a história)
uv run capi-etl --mode full

# Carga incremental (últimos ETL_SINCE_DAYS dias — padrão)
uv run capi-etl --mode incremental

# Apenas Jira
uv run capi-etl --mode incremental --only jira

# Apenas GitHub
uv run capi-etl --mode incremental --only github
```

Também é possível executar como módulo Python:

```bash
uv run python -m capi_etl --mode full
```

## Exit codes

| Código | Significado |
|---|---|
| `0` | Sucesso |
| `1` | Erro durante o pipeline (ver logs) |
| `2` | Configuração inválida (variável de ambiente ausente) |

## Rodando no TARGIT InMemoryETL

O TARGIT deve chamar o executável com as variáveis de ambiente definidas no ambiente de execução:

```
capi-etl --mode incremental
```

Para o primeiro carregamento histórico use `--mode full`.

Certifique-se de que `uv sync` foi executado no host onde o TARGIT rodará o script.

## Rodando os testes

```bash
uv run pytest -v
```

## Modelo de dados

| Tabela | Chave primária | Descrição |
|---|---|---|
| `fato_issues` | `issue_key` | Issues do Jira com lead time e flag de bug |
| `fato_changelog` | `id` (surrogate) | Transições de status com tempo no status |
| `fato_pull_requests` | `pr_id` (`repo#número`) | PRs com time-to-merge, reviews e comentários |
| `fato_commits` | `commit_hash` | Commits vinculados a PRs |

As tabelas são criadas automaticamente na primeira execução. O pipeline usa `INSERT ... ON CONFLICT DO UPDATE` (upsert), tornando todas as execuções idempotentes.

## Métricas disponíveis para o BI (seção 5 do PRD)

| Métrica | Fonte |
|---|---|
| Volume de bugs | `fato_issues WHERE is_bug = true` |
| Lead Time (inovação) | `fato_issues.lead_time_dias WHERE NOT is_bug` |
| MTTR | `fato_issues.lead_time_dias WHERE is_bug` |
| Iterações de PR | `fato_pull_requests.num_comments + num_reviews` |
| Time to Merge | `fato_pull_requests.time_to_merge_horas` |
| Frequência de commits | `fato_commits GROUP BY author_name, DATE_TRUNC('month', commit_date)` |
