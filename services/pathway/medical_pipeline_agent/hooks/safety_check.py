#!/usr/bin/env python3
"""
PreToolUse hook: validates Bash commands before execution.
Blocks destructive operations on Neo4j and filesystem.

Exit codes:
  0 = allow (stdout shown to user)
  2 = block (stderr fed to Claude as error)
"""
import sys
import json
import os
import re

# Read tool input from stdin
tool_input = sys.stdin.read().strip()

# Dangerous patterns that MUST be blocked
BLOCKED_PATTERNS = [
    # Neo4j destructive operations
    r'DETACH\s+DELETE',
    r'DELETE\s+\w',
    r'DROP\s+INDEX',
    r'DROP\s+CONSTRAINT',
    r'REMOVE\s+\w+\.embedding',
    # Filesystem destructive
    r'rm\s+-rf\s+/',
    r'rm\s+-rf\s+\.',
    r'rm\s+-rf\s+\*',
    r'rmdir\s+/',
    # Git destructive
    r'git\s+push\s+--force',
    r'git\s+push\s+-f\b',
    r'git\s+reset\s+--hard',
    r'git\s+clean\s+-fd',
    # Data exposure
    r'cat\s+\.env',
    r'echo\s+\$\{?AZURE',
    r'echo\s+\$\{?API_KEY',
    r'printenv',
]

# Warning patterns (allow but warn)
WARNING_PATTERNS = [
    r'neo4j.*MERGE',
    r'neo4j.*CREATE',
    r'neo4j.*SET\s',
    r'pip\s+install',
    r'npm\s+install',
]

def check_command(cmd: str) -> tuple[bool, str]:
    """Returns (blocked: bool, reason: str)."""
    cmd_upper = cmd.upper()

    for pattern in BLOCKED_PATTERNS:
        if re.search(pattern, cmd, re.IGNORECASE):
            return True, f"BLOCKED: Pattern '{pattern}' matches destructive operation"

    return False, ""

def check_warnings(cmd: str) -> str:
    """Returns warning message if applicable."""
    warnings = []
    for pattern in WARNING_PATTERNS:
        if re.search(pattern, cmd, re.IGNORECASE):
            warnings.append(f"Neo4j write operation detected: {pattern}")
    return "; ".join(warnings) if warnings else ""

if __name__ == "__main__":
    try:
        # Tool input comes as JSON on stdin
        data = json.loads(tool_input) if tool_input.startswith('{') else {"command": tool_input}
        command = data.get("command", tool_input)

        blocked, reason = check_command(command)
        if blocked:
            print(reason, file=sys.stderr)
            sys.exit(2)  # Block the operation

        warning = check_warnings(command)
        if warning:
            print(f"[SAFETY] {warning}")

        sys.exit(0)  # Allow

    except Exception as e:
        # On error, allow but log
        print(f"[SAFETY] Hook error (allowing): {e}", file=sys.stderr)
        sys.exit(0)
