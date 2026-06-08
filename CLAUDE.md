# Claude Code Project Context: sport_lakehouse

A reference lakehouse on Databricks built on fake Norwegian sports data.
Designed as a best-practices demo for workshops and client engagements —
covering DABS deployment, Spark Declarative Pipelines, and medallion architecture.

## Technology Stack

- **Deployment**: Databricks Asset Bundles (DABS) — `knowit-sandbox` profile
- **Pipelines**: Spark Declarative Pipelines (SDP / Lakeflow)
- **CI/CD**: GitHub Actions (planned — not yet wired)
- **IDE**: VS Code + Claude Code

## Skills

**Start here**: `sport-lakehouse-context` — full project context, layer logic, standards

| Category | Skills |
|----------|--------|
| **Pipelines** | `databricks-spark-declarative-pipelines` |
| **Bundles** | `databricks-bundles` |
| **Genie** | `databricks-genie` |
| **Metric views** | `databricks-metric-views` |
| **Jobs** | `databricks-jobs` |

## Project Structure

```
sport_lakehouse/
├── databricks.yml              # Bundle root config
├── resources/
│   ├── jobs/                   # Job definitions (YAML)
│   └── pipelines/              # Pipeline definitions (YAML)
└── src/
    ├── landing/                # Daily ingest notebooks
    ├── silver/                 # SDP pipeline notebooks
    └── gold/                   # (planned)
```

## Quick Start

```bash
databricks bundle deploy --target dev_julia --profile knowit-sandbox                 # deploy to personal sandbox
databricks bundle deploy --profile knowit-sandbox                                    # deploy to shared dev (CI/CD does this on merge)
databricks bundle run landing_daily --target dev_julia --profile knowit-sandbox      # run daily job
databricks bundle run landing_daily --target dev_julia --profile knowit-sandbox --params run_date=2025-06-01
databricks bundle validate --profile knowit-sandbox                                  # validate config
databricks bundle sync --profile knowit-sandbox                                      # sync without deploy
```

## Critical Gotchas

- **Confirmation required**: never run `git commit`, `git push`, or `databricks bundle deploy` without explicit approval
- **Three-part table names**: always `catalog.schema.table` — never unqualified
- **Bronze is append-only**: never modify bronze directly — transformations in silver
- **No hardcoding**: warehouse IDs, catalog names, workspace URLs use variables/widgets
- **dev_live needs confirmation**: never deploy to prod-equivalent target without approval
