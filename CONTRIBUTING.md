# Contributing

## Starting a work session

```bash
# 1. Open VS Code in the project folder
cd /Users/juliaforde/Documents/databricks-projects/knowit-sandbox/sport_lakehouse
code .

# 2. In the VS Code terminal — get latest and create a feature branch
git checkout main
git pull origin main
git checkout -b feature/what-you-are-doing

# 3. Open a second terminal panel and start Claude
claude

# 4. In Claude — load project context
/sport-lakehouse-context

# 5. If Databricks CLI needs authentication (first time or token expired)
databricks auth login --profile knowit-sandbox
```

---

## How changes get deployed

```
feature branch  →  PR to main  →  merge  →  auto deploy to dev
                                            manual trigger → dev_live
```

- **Local dev**: test against dev manually from your laptop
- **PR**: GitHub validates the bundle automatically (`bundle validate`)
- **Merge to main**: GitHub automatically deploys to `dev`
- **dev_live**: manual trigger only — go to Actions → Run workflow

## Day-to-day workflow

### Starting a new task

```bash
# Always start from an up-to-date main
git checkout main
git pull

# Create a feature branch
git checkout -b feature/your-description
```

Branch naming:
- `feature/add-gold-layer`
- `fix/scd2-key-generation`
- `chore/update-bundle-config`

### While developing

Each developer has a personal sandbox target in `databricks.yml` — deploy there to test
your changes without affecting the shared `dev` environment.

Add your own target by copying the `dev_julia` block in `databricks.yml` and updating
the target name and `root_path` email to your own.

```bash
# Deploy to YOUR personal sandbox (replace dev_julia with your target name)
databricks bundle deploy --target dev_julia --profile knowit-sandbox

# Run a job in your sandbox
databricks bundle run landing_daily --target dev_julia --profile knowit-sandbox

# Validate config
databricks bundle validate --profile knowit-sandbox
```

The shared `dev` target is only deployed to by CI/CD on merge to main — don't deploy
there manually.

Commit often — small, logical chunks:

```bash
git add <specific files>
git commit -m "feat: short description of what and why"
```

Commit message types: `feat`, `fix`, `refactor`, `chore`, `docs`

### Multiple tasks in parallel

Each task lives on its own branch — they are completely independent:

```bash
# Park task A (just commit your work-in-progress)
git add .
git commit -m "chore: wip task A"

# Switch to task B
git checkout main && git pull
git checkout -b feature/task-b

# ... finish task B, open PR, merge ...

# Come back to task A
git checkout feature/task-a
# continue where you left off
```

### Opening a PR

When your feature is ready:

```bash
git push origin feature/your-description
```

Then open a PR on GitHub. The `bundle validate` check runs automatically — you'll see it on the PR. Only merge when it passes.

PR checklist:
- [ ] Bundle validates cleanly
- [ ] Tested manually against dev
- [ ] Follows naming conventions
- [ ] No hardcoded catalog names, warehouse IDs, or workspace URLs

### After merging

GitHub automatically deploys to `dev`. Check the **Actions** tab to confirm it succeeded.

To deploy to `dev_live`:
1. Go to **Actions** → **Databricks Bundle CI/CD**
2. Click **Run workflow**
3. Select `dev_live`
4. Click **Run workflow** again

## Git conventions

See the `github-workflow` skill for full commit message format, PR structure, and review process.

## Authentication

**Local**: pass `--profile knowit-sandbox` to all bundle commands (see examples above)

**CI/CD**: uses `DATABRICKS_TOKEN` GitHub secret — no action needed

> **TODO**: Replace PAT with a Databricks Service Principal when available.
> See `.github/README.md` for details.
