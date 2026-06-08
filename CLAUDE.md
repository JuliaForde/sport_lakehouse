# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Never
- Never run `git commit`, `git push`, or `databricks bundle deploy` without explicit confirmation from me
- Never use unqualified table names — always `catalog.schema.table` (three-part fully qualified)
- Never hardcode warehouse IDs, catalog names, or workspace URLs — always use variables or widgets
- Never run `bundle deploy --target dev_live` or any prod-equivalent target without explicit confirmation
- Never modify bronze tables directly — bronze is append-only, transformations happen in silver

## Current state
- Silver pipeline: addresses, clubs, members, sport_types, club_sports, affiliations, memberships, competitions, membership_payments, participation, competition_waitlist, results (SDP/Lakeflow)
- Gold: metric views — manual deploy via notebook (DDL wraps YAML, executed via `spark.sql`)
- Genie: "Norwegian Sports" space — exported as `.genie.json` artifact, promoted via SDK export/import script
- No prod environment yet — everything runs in dev/sandbox (`knowit-sandbox` profile)
- No CI/CD yet — deployments are manual via CLI or VS Code extension

## Direction (where this is heading)
- We need to make the Bronze and Silver layer, more solid, currently not tested well especially lookups, keys and Scd2 
- CI/CD will use GitHub Actions
- We want to build the guold layer of the solution also using  SDP(spark declarative pipelines)
-We want to add metric views as a base for out semantic layer that can be used as source of truth for dashboards & genie spaces 
- we want to build geni spaces + dashaboard along with best practices for deploying them

## Databricks CLI commands

All commands run from `sport_lakehouse/` using the `knowit-sandbox` profile.

```bash
# Deploy bundle to dev (default target)
databricks bundle deploy

# Deploy to dev_live (production-mode target)
databricks bundle deploy --target dev_live

# Run the daily landing job manually
databricks bundle run landing_daily

# Run with a specific date
databricks bundle run landing_daily --params run_date=2025-06-01

# Validate bundle config without deploying
databricks bundle validate

# Sync local files to workspace (without full deploy)
databricks bundle sync