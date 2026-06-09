# Skills Backlog — sport_lakehouse

## Candidates

- [ ] **Layer logic** — detailed rules for what belongs in bronze vs silver vs gold. Hold until SDP patterns stabilize.
- [ ] **Naming + key generation standards** — concrete rules for table/column names and key generation patterns.
- [ ] **SCD2 pattern** — the one correct implementation for this project. Hold until validated in SDP.
- [ ] **Setup checklist** — AI-dev kit, DABS integrations, what a new contributor needs before touching the code.
- [ ] **DABS multi-target pattern** — personal sandbox (dev_julia) + shared dev + dev_live targets, allow_duplicate_names for pipelines, mode: production for shared targets. Stabilized today.
- [ ] **GitHub Actions + DABS auth** — remove profile from databricks.yml, use DATABRICKS_TOKEN + DATABRICKS_HOST env vars in CI. PAT now, Service Principal when available.
- [x] **Skill sync automation** — resolved with a symlink: `~/.claude/skills/sport-lakehouse-context` points to the project copy, so there's one source of truth.

## Done

- [x] `sport-lakehouse-context` — project context skill, written and acid-tested 2026-06-08
