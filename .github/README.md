# CI/CD — GitHub Actions

## What is .github/workflows/?

GitHub automatically reads any `.yml` file in this directory and treats it as an
automated pipeline. When you push code or open a PR, GitHub runs the steps defined
there on its own servers — no manual action needed on your laptop.

This means Databricks deployments no longer happen manually from VS Code. They go
through Git, so every deploy is tracked, reviewed, and repeatable.

## How authentication works

The workflow authenticates to Databricks using a secret stored in GitHub
(Settings → Secrets and variables → Actions).

**Current method: Personal Access Token (PAT)**
Simple to set up, but tied to a personal user account. If the token expires or the
user leaves, the pipeline breaks.

**Future: Service Principal (TODO)**
A dedicated non-personal identity for CI/CD. More robust for shared/production use.
When a service principal is available, replace `DATABRICKS_TOKEN` with
`DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET` in the workflow file.

## Required setup (one time)

1. Generate a PAT in Databricks: **Settings → Developer → Access tokens → Generate new token**
2. Add it to GitHub: **Repo → Settings → Secrets and variables → Actions → New repository secret**
   - Name: `DATABRICKS_TOKEN`
   - Value: the token you just generated
3. Create a `dev_live` GitHub Environment:
   **Repo → Settings → Environments → New environment → name: `dev_live`**
   Set **Deployment branches and tags** to **Protected branches only**.

   > **Note:** Required reviewers (manual approval gate) is a GitHub Team/Enterprise feature
   > and not available on free personal accounts. On a free plan, the manual trigger
   > (`workflow_dispatch`) itself acts as the gate — someone has to explicitly click
   > "Run workflow" in GitHub Actions to deploy to `dev_live`. Upgrade to GitHub Team
   > ($4/month) to add a required reviewer approval step.

## Workflows

| File | What it does |
|------|-------------|
| `bundle-cicd.yml` | Validates on PR, deploys to `dev` on merge to main, manual deploy to `dev_live` |
