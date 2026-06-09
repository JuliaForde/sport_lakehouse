---
name: github-workflow
description: Git operations, commit messages, PR creation, and branch management patterns
---

name: github-workflow
description: Git operations, commit messages, PR creation, and branch management patterns
version: 1.0.0
type: workflow

commit_messages:
  format:
    structure: |
      <type>: <subject>

      <body (optional)>

      <footer (optional)>

    types:
      - "feat: New feature"
      - "fix: Bug fix"
      - "docs: Documentation changes"
      - "style: Formatting, missing semicolons, etc."
      - "refactor: Code restructuring without behavior change"
      - "perf: Performance improvements"
      - "test: Adding or updating tests"
      - "chore: Build, CI, dependencies, etc."

    subject_rules:
      - Start with lowercase verb (add, fix, update, remove)
      - Use imperative mood ("add feature" not "added feature")
      - No period at end
      - Max 50 characters
      - Be specific but concise

    body_rules:
      - Wrap at 72 characters
      - Explain "why" not "what" (code shows what)
      - Include context and reasoning
      - Reference issues/tickets if relevant

    examples:
      good:
        - "feat: add Gerber X3 format support"
        - "fix: correct drill coordinate parsing for metric units"
        - "refactor: extract validation logic into separate service"
        - "docs: update API examples with error handling"

      bad:
        - "Updated stuff" (too vague)
        - "Fixed bug" (which bug?)
        - "WIP" (not a finished commit)
        - "asdfasdf" (meaningless)

  conventions:
    co_authored_by:
      - Add when pairing or AI-assisted
      - Format: "Co-Authored-By: Name <email>"
      - Example: "Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"

    breaking_changes:
      - Use "BREAKING CHANGE:" in footer
      - Explain what broke and migration path
      - Consider major version bump

    references:
      - Link to issues: "Fixes #123" or "Closes #456"
      - Link to PRs: "See PR #789"
      - Link to docs: "See docs/architecture.md"

branch_naming:
  patterns:
    feature: "feature/short-description"
    bugfix: "fix/short-description"
    hotfix: "hotfix/critical-issue"
    release: "release/v1.2.0"
    experiment: "experiment/idea-name"

  examples:
    - "feature/gerber-x3-support"
    - "fix/drill-coordinate-parsing"
    - "hotfix/security-vulnerability"
    - "refactor/extract-validation-service"

  rules:
    - Use lowercase with hyphens
    - Keep short but descriptive
    - Include ticket number if available: "feature/PCB-123-drill-parsing"
    - Avoid personal branches (feature/thor/something)

pull_requests:
  title:
    - Mirror commit message format (type: description)
    - Keep under 70 characters
    - Use sentence case
    - No period at end

  description:
    structure: |
      ## Summary
      Brief overview (2-3 sentences or bullet points)

      ## Changes
      - Key change 1
      - Key change 2
      - Key change 3

      ## Testing
      - [ ] Unit tests added/updated
      - [ ] Integration tests pass
      - [ ] Manual testing completed

      ## Screenshots (if UI changes)
      [Before/after images]

      ## Related Issues
      Fixes #123

      ## Notes
      Any deployment considerations, breaking changes, or follow-up work

  best_practices:
    - Keep PRs focused (one concern per PR)
    - Include tests with code changes
    - Update documentation in same PR
    - Add screenshots for UI changes
    - Request specific reviewers
    - Respond to review comments promptly
    - Mark conversations resolved when addressed

  size_guidelines:
    - Ideal: < 400 lines changed
    - Good: 400-800 lines
    - Large: 800-1200 lines (split if possible)
    - Too large: > 1200 lines (definitely split)

review_process:
  as_author:
    before_submitting:
      - Review your own PR first
      - Run all tests locally
      - Check for console errors
      - Verify documentation is updated
      - Ensure commit messages are clear

    responding_to_feedback:
      - Address all comments (don't ignore)
      - Ask questions if unclear
      - Push fixup commits (squash before merge)
      - Mark conversations resolved
      - Thank reviewers for feedback

  as_reviewer:
    what_to_check:
      - Does it solve the stated problem?
      - Are there tests?
      - Is documentation updated?
      - Any security concerns?
      - Performance implications?
      - Code clarity and maintainability
      - Follows project conventions?

    how_to_comment:
      - Be specific and constructive
      - Explain "why" not just "what to change"
      - Distinguish between required and optional
      - Use suggestions for small fixes
      - Praise good solutions
      - Ask questions, don't demand

    approval_criteria:
      - Tests pass
      - Code is understandable
      - No obvious bugs or security issues
      - Documentation is adequate
      - Follows project standards

git_workflow:
  daily_workflow:
    start_work:
      - "git pull origin main"
      - "git checkout -b feature/my-feature"
      - Make changes
      - "git add <specific files>"
      - "git commit -m 'feat: description'"

    during_work:
      - Commit frequently (small, logical chunks)
      - Push to remote regularly (backup)
      - Pull from main periodically (stay up to date)
      - Rebase if main has moved

    finish_work:
      - "git pull origin main"
      - "git rebase main" (if needed)
      - "git push origin feature/my-feature"
      - Create PR via gh CLI or web UI

  merge_strategies:
    squash_merge:
      when: Feature branches with many small commits
      effect: Clean main history, one commit per PR
      use_for: Most feature work

    merge_commit:
      when: Want to preserve branch history
      effect: Maintains all commits, adds merge commit
      use_for: Large features, release branches

    rebase_merge:
      when: Linear history desired
      effect: Replays commits on main
      use_for: Small PRs with clean commits

  conflict_resolution:
    steps:
      - "git fetch origin"
      - "git rebase origin/main"
      - Resolve conflicts in files
      - "git add <resolved files>"
      - "git rebase --continue"
      - "git push --force-with-lease"

    best_practices:
      - Understand both changes before resolving
      - Test after resolving
      - Ask original author if unsure
      - Don't force push to shared branches

hooks:
  pre_commit:
    - Run linters (checkstyle, eslint, etc.)
    - Run fast tests
    - Check formatting
    - Verify no debug code
    - Fail if issues found

  commit_msg:
    - Validate commit message format
    - Check subject length
    - Ensure body wrapping

  pre_push:
    - Run full test suite
    - Check no uncommitted changes
    - Verify branch is up to date

safety_rules:
  never:
    - Force push to main/master
    - Commit sensitive data (keys, passwords, tokens)
    - Commit large binaries (use Git LFS)
    - Amend pushed commits (breaks history)
    - Push broken code
    - Skip tests with --no-verify

  always:
    - Review changes before committing
    - Write meaningful commit messages
    - Test before pushing
    - Keep commits focused
    - Update documentation with code

  dangerous_operations:
    - "git reset --hard" (loses uncommitted work)
    - "git push --force" (rewrites history, breaks others)
    - "git clean -fd" (deletes untracked files)
    - "git rebase" on shared branches (confuses collaborators)

gh_cli:
  common_commands:
    pr_create:
      - "gh pr create --title 'feat: description' --body 'Summary...'"
      - "gh pr create --fill" (use commit message)
      - "gh pr create --draft" (work in progress)

    pr_view:
      - "gh pr view" (current branch)
      - "gh pr view 123" (specific PR)
      - "gh pr view --web" (open in browser)

    pr_review:
      - "gh pr review --approve"
      - "gh pr review --comment --body 'Looks good!'"
      - "gh pr review --request-changes"

    issue_work:
      - "gh issue create --title 'Bug: description'"
      - "gh issue list --assignee @me"
      - "gh issue view 123"

best_practices:
  commit_hygiene:
    - One logical change per commit
    - Commits should be atomic (buildable)
    - No "WIP" or "fix typo" commits in main
    - Squash fixup commits before merging

  branch_hygiene:
    - Delete branches after merging
    - Don't reuse branch names
    - Keep branches short-lived (< 1 week)
    - Rebase on main regularly

  collaboration:
    - Pull before starting work
    - Push at end of day (backup)
    - Communicate large changes
    - Review PRs promptly (< 24 hours)

examples:
  good_commit:
    message: |
      feat: add Gerber X3 format support

      Implements parser for Gerber X3 format with embedded attributes.
      Uses state machine approach for robust parsing of complex files.

      Includes:
      - X3 attribute parsing (TF, TA, TD commands)
      - Backward compatibility with X2 format
      - Comprehensive test suite with real-world files

      Fixes #234

      Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>

  good_pr:
    title: "feat: add Gerber X3 format support"
    body: |
      ## Summary
      Adds support for Gerber X3 format with embedded attributes

      ## Changes
      - Implemented X3 attribute parser
      - Added state machine for robust parsing
      - Maintained backward compatibility with X2
      - Added 50+ test cases with real-world files

      ## Testing
      - [x] Unit tests added (50+ cases)
      - [x] Integration tests pass
      - [x] Manual testing with production files

      ## Performance
      Parsing speed: 2.3 MB/s (same as X2)

      ## Related Issues
      Fixes #234

      🤖 Generated with Claude Code

related_skills:
  - documentation-style
  - writing-patterns
  - verification-patterns
  - create-pr (Elprint)
  - prepare-pr (Elprint)

usage_notes: |
  These conventions apply to all Git operations across projects.

  Key principles:
  - Commits tell a story (make it readable)
  - PRs should be reviewable (not too large)
  - Main branch is always deployable
  - History is valuable (don't rewrite shared history)

  When in doubt:
  - Smaller commits are better
  - More specific messages are better
  - Focused PRs are better
  - Clear communication is better

> **Also exists at**: `~/.claude/skills/github-workflow/` — update both when changing.
