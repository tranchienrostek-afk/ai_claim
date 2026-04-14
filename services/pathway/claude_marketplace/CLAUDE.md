# CLAUDE.md

This folder contains Pathway's project-local Claude plugin marketplace.

Path:

- `pathway/notebooklm/claude_marketplace/`

Its purpose is to make Claude better at understanding and operating within Pathway by using local plugin metadata, skills, hooks, and commands.

## Use This Folder For

- project-local plugin definitions
- skills that sharpen Pathway-specific reasoning
- hooks that protect high-risk edits
- helper commands that improve local operator workflow

## Do Not Use This Folder For

- core FastAPI business logic
- runtime state storage
- large architecture docs that belong in `CLAUDE.md` or `CLAUDE_PROJECT_MAP.md`

## Local Rules

- keep skills narrow and task-oriented
- keep hooks lightweight, explainable, and safety-focused
- avoid hooks that create noisy false blocks
- prefer promoting stable guidance into a skill over repeating giant prompt text in every call
- commands should help operators or Claude sessions; they should not replace the backend runtime contract

## Expected Structure

- `.claude-plugin/` for plugin metadata
- `plugins/` for project-local plugin implementations

## Runtime Note

Changes here usually affect the next Claude invocation.
They typically do not require rebuilding the Pathway image when bind-mounted, though app-specific Python code elsewhere may still require API restart.
