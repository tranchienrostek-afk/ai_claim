#!/usr/bin/env bash
set -euo pipefail

cat <<'EOF'
{
  "continue": true,
  "suppressOutput": true,
  "systemMessage": "Project plugin pathway-intelligence is active. Treat /app as the canonical Pathway workspace. For deep repo claims, read /app/CLAUDE.md and /app/CLAUDE_PROJECT_MAP.md first. Use pathway-runtime-context for architecture and repo wiring, pathway-orchestration-gate for duet or decision-gate protocol questions, pathway-pipeline-agent for Phase 0-4 ingestion workflow, run state, experience memory, and operator resumes, pathway-operator-review for paused human-review flow and dashboard control reasoning, and pathway-change-safety before proposing code changes."
}
EOF
