# Claude Code Project Context: sport_lakehouse

A reference lakehouse on Databricks built on fake Norwegian sports data.
Designed as a best-practices demo for workshops and client engagements —
covering DABS deployment, Spark Declarative Pipelines, and medallion architecture.

## Technology Stack

- **Deployment**: Databricks Asset Bundles (DABS) — `Databricks_Free` profile
- **Pipelines**: Spark Declarative Pipelines (SDP / Lakeflow)
- **CI/CD**: GitHub Actions — validate on PR, deploy to `live` on merge to main
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
Lakehouse_FreeEdition/
├── bundles/
│   ├── sport_lakehouse/            # Sport lakehouse bundle
│   │   ├── databricks.yml          # Bundle root config
│   │   ├── resources/
│   │   │   ├── jobs/               # Job definitions (YAML)
│   │   │   └── pipelines/          # Pipeline definitions (YAML)
│   │   └── src/
│   │       ├── landing/            # Daily ingest notebooks
│   │       ├── bronze/             # Bronze pipeline
│   │       └── pipeline/           # Silver SDP pipeline notebooks
│   └── fjordarcade/                # Fjordarcade bundle (planned)
│       └── databricks.yml
└── CLAUDE.md
```

## Quick Start

All bundle commands must be run from the `bundles/sport_lakehouse/` directory:

```bash
cd bundles/sport_lakehouse
databricks bundle deploy --target dev_julia --profile Databricks_Free                 # deploy to personal sandbox (dev_julia catalog)
databricks bundle deploy --target live --profile Databricks_Free                      # deploy to live (sport_lakehouse catalog) — CI/CD does this on merge
databricks bundle run landing_daily --target dev_julia --profile Databricks_Free      # run daily job
databricks bundle run landing_daily --target dev_julia --profile Databricks_Free --params run_date=2025-06-01
databricks bundle validate --target live --profile Databricks_Free                    # validate live config
databricks bundle sync --target dev_julia --profile Databricks_Free                   # sync without deploy
```

## Session Start

At the start of every session:
1. Run `git branch --show-current`. If the current branch is `main`:
   - Give a one-time soft reminder: "Heads up — you're on `main`. Remember to pull and branch before making any changes."
   - Do not repeat the reminder unless the developer asks you to make a code change
   - If they ask for a code change while still on `main`, remind them to branch first before proceeding
2. Load `sport-lakehouse-context` for project context.
3. Ask: "What are we working on today?" and load only the skills relevant to the answer:
   - Pipelines / bronze / silver → `databricks-spark-declarative-pipelines`
   - Bundle deploy / CI/CD / targets → `databricks-bundles`
   - Jobs / scheduling → `databricks-jobs`
   - Workspace / profiles → `databricks-config`
   - Catalog / schema / permissions → `databricks-unity-catalog`
   - Streaming → `databricks-spark-structured-streaming`
   - Dashboards / Genie → `databricks-genie`
   - Metric views → `databricks-metric-views`
   - SQL / queries → `databricks-dbsql`

## Critical Gotchas

- **Confirmation required**: never run `git commit`, `git push`, or `databricks bundle deploy` without explicit approval
- **Three-part table names**: always `catalog.schema.table` — never unqualified
- **Bronze is append-only**: never modify bronze directly — transformations in silver
- **No hardcoding**: warehouse IDs, catalog names, workspace URLs use variables/widgets
- **live needs confirmation**: never run `databricks bundle deploy --target live` manually without approval — CI/CD handles this on merge to main
