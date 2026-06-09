---
name: sport-lakehouse-context
description: Full context for the sport_lakehouse project — intent, tech stack, current state, layer logic, and standards. Load this at the start of every session.
tags: [context, onboarding, architecture, medallion, databricks]
---

# sport_lakehouse — Project Context

## What this project is

A reference lakehouse implementation on Databricks, built on a fake Norwegian sports dataset. Primary purpose: a demo and teaching tool for workshops and client engagements. It should demonstrate best practices for how to architect and implement a lakehouse on Databricks — someone attending a workshop or a new client should be able to look at this and understand how to do it right.

**The author is an experienced Databricks architect and consultant** who has built many lakehouses — mostly using notebooks and Azure DevOps. This project deliberately uses newer tooling (DABS, Spark Declarative Pipelines) that is less familiar, so Claude should help bridge that gap: explain DABS/SDP patterns, point out where they differ from notebook-based approaches, and suggest idiomatic usage.

## Tech stack

- **IDE**: VS Code + Claude Code
- **Deployment**: Databricks Asset Bundles (DABS) — `databricks bundle deploy` from `sport_lakehouse/` using the `knowit-sandbox` profile
- **Pipelines**: Spark Declarative Pipelines (SDP / Lakeflow) for silver layer transformations
- **CI/CD**: GitHub Actions — **not yet wired up**. Currently bundle deploy happens manually from VS Code; git commits are separate. Goal: deploy goes through GitHub Actions.
- **Profile**: `knowit-sandbox` (dev/sandbox only — no prod environment yet)

## Current state (as of 2026-06-08)

| Layer | Status |
|-------|--------|
| Landing (bronze ingest) | Built — daily notebooks for membership payments, participation, waitlist, results. Bootstrap notebook for full seed. |
| Bronze | Built — append-only raw tables including weather tables |
| Silver | Built but not solid — SDP pipeline covers addresses, clubs, members, sport_types, club_sports, affiliations, memberships, competitions, membership_payments, participation, competition_waitlist, results. Lookups, keys, and SCD2 logic need hardening. |
| Gold | Not yet built — planned as SDP |
| Metric views | Partial — manual deploy via notebook (DDL wraps YAML, executed via spark.sql) |
| Genie space | Built — "Norwegian Sports", exported as `.genie.json`, promoted via SDK export/import script |
| Dashboards | Not yet built |

**Immediate priorities:**
1. Make bronze and silver more solid — especially lookup logic, key generation, and SCD2
2. Wire CI/CD: bundle deploy through GitHub Actions
3. Extend to gold layer, metric views, dashboards

## Medallion layer logic

### Bronze (landing → bronze)
- **Append-only** — never modify bronze tables directly
- Raw data as close to source as possible
- Minimal transformation — just enough to land the data reliably
- Schema enforcement happens here, not business logic

### Silver (bronze → silver)
- All transformations use **Spark Declarative Pipelines (SDP)** — not notebooks
- Business logic lives here: deduplication, SCD2, lookups, joins, key generation
- SCD2 should follow a consistent pattern — do not implement it differently per table
- Keys must follow the project naming and generation standards (see below)

### Gold (silver → gold) — planned
- Will use SDP (same tooling as silver)
- Aggregations, business-level entities, pre-joined wide tables for reporting
- No raw IDs exposed — use business keys

### Metric views
- Defined in YAML, deployed via DDL notebook wrapping `spark.sql`
- Serve as the semantic layer — source of truth for dashboards and Genie spaces

## Standards that matter

**Naming**: Consistency is the top code review issue. Follow established naming conventions — table names, column names, key columns, task keys in YAML. When in doubt, match what already exists in the project.

**Key generation**: Keys must follow the project's established pattern. Do not invent new approaches.

**Tool consistency**: The same problem should be solved the same way across all tables. If silver uses SDP, all silver uses SDP. If SCD2 is implemented one way, all SCD2 follows that pattern. This project is a reference — inconsistency undermines its purpose.

**Table references**: Always three-part fully qualified — `catalog.schema.table`. Never unqualified names.

**No hardcoding**: Warehouse IDs, catalog names, workspace URLs always use variables or widgets.

**Bundle config**: Never run `bundle deploy --target dev_live` or any prod-equivalent target without explicit confirmation.
