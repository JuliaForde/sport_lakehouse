# Contributing

## Workspaces

This repo deploys to two Databricks workspaces:

| Workspace | Purpose | Targets |
|-----------|---------|---------|
| **Knowit sandbox** (`dbc-8c27291b-31c5`) | Company — training and demos | `knowit_dev_julia`, `knowit_dev`, `knowit_live` |
| **Free edition** (`dbc-ef88803f-e606`) | Julia's personal sandbox — safe to experiment | `dev_julia`, `live` |

CI/CD only deploys to the knowit workspace. The free edition is deployed manually from the laptop.

---

## Starting a work session

```bash
# 1. Get latest and create a feature branch
git checkout main
git pull origin main
git checkout -b feature/what-you-are-doing

# 2. Start Claude and load project context
claude
/sport-lakehouse-context

# 3. Authenticate if needed (first time or token expired)
databricks auth login --profile knowit-sandbox
databricks auth login --profile Databricks_Free   # Julia only
```

All bundle commands must be run from `bundles/sport_lakehouse/`:

```bash
cd bundles/sport_lakehouse
```

---

## How changes get deployed

```
feature branch  →  PR to main  →  merge  →  auto deploy to knowit_dev
                                            manual trigger → knowit_live
```

- **PR**: GitHub validates the bundle automatically
- **Merge to main**: GitHub auto-deploys to `knowit_dev` (shared dev/training)
- **knowit_live**: manual trigger only — go to Actions → Run workflow

---

## Day-to-day workflow

### Starting a new task

```bash
git checkout main && git pull
git checkout -b feature/your-description
```

Branch naming: `feature/`, `fix/`, `chore/`

### While developing — knowit sandbox

Each developer has a personal sandbox target. Deploy there to test without affecting the shared `knowit_dev` environment.

To add your own target: copy the `knowit_dev_julia` block in `databricks.yml`, change the target name and `root_path` to your own email.

```bash
# Deploy to your personal sandbox
databricks bundle deploy --target knowit_dev_julia --profile knowit-sandbox

# Run a job
databricks bundle run landing_daily --target knowit_dev_julia --profile knowit-sandbox

# Validate config
databricks bundle validate --target knowit_dev_julia --profile knowit-sandbox
```

The `knowit_dev` target is CI/CD only — do not deploy there manually.

### While developing — free edition (Julia only)

```bash
# Deploy to personal free edition sandbox
databricks bundle deploy --target dev_julia --profile Databricks_Free

# Deploy to free edition live
databricks bundle deploy --target live --profile Databricks_Free
```

### Committing

```bash
git add <specific files>
git commit -m "feat: short description of what and why"
```

Commit types: `feat`, `fix`, `refactor`, `chore`, `docs`

### Opening a PR

```bash
git push origin feature/your-description
```

Open a PR on GitHub — `bundle validate` runs automatically. Only merge when it passes.

PR checklist:
- [ ] Bundle validates cleanly
- [ ] Tested manually against personal sandbox
- [ ] Follows naming conventions
- [ ] No hardcoded catalog names, warehouse IDs, or workspace URLs

### After merging

GitHub auto-deploys to `knowit_dev`. Check the **Actions** tab to confirm.

To deploy to `knowit_live`:
1. Go to **Actions** → **Databricks Bundle CI/CD**
2. Click **Run workflow** → select `knowit_live` → **Run workflow**

---

## Git conventions

See the `github-workflow` skill for commit message format, PR structure, and review process.

---

## Authentication

**Local (knowit)**: `--profile knowit-sandbox`

**Local (free edition)**: `--profile Databricks_Free`

**CI/CD**: uses the `DATABRICKS_TOKEN` GitHub secret (PAT for the knowit workspace). To rotate: generate a new token in the knowit workspace under Settings → Developer → Access tokens, then update the secret at `github.com/JuliaForde/sport_lakehouse/settings/secrets/actions`.

> **TODO**: Replace PAT with a Databricks Service Principal when available.
