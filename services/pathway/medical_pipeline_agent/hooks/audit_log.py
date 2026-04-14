#!/usr/bin/env python3
"""
PostToolUse hook: logs all write operations for audit trail.
Appends to data/pipeline_runs/audit.jsonl
"""
import sys
import json
import os
from datetime import datetime

AUDIT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "pipeline_runs")
AUDIT_FILE = os.path.join(AUDIT_DIR, "audit.jsonl")

def log_operation(tool_name: str, tool_input: str):
    """Append operation to audit log."""
    os.makedirs(AUDIT_DIR, exist_ok=True)

    entry = {
        "timestamp": datetime.now().isoformat(),
        "tool": tool_name,
        "input_preview": tool_input[:500] if tool_input else "",
    }

    try:
        with open(AUDIT_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass  # Don't block operations due to logging failures

if __name__ == "__main__":
    try:
        raw = sys.stdin.read().strip()
        data = json.loads(raw) if raw.startswith('{') else {}
        tool_name = data.get("tool_name", "unknown")
        tool_input = json.dumps(data.get("tool_input", ""), ensure_ascii=False)
        log_operation(tool_name, tool_input)
    except Exception:
        pass

    sys.exit(0)  # Never block on audit
