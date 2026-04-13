from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class DomainPolicy:
    raw: dict[str, Any]

    @classmethod
    def from_file(cls, path: Path) -> "DomainPolicy":
        return cls(json.loads(path.read_text(encoding="utf-8")))

    @property
    def allowed_builtin_tools(self) -> list[str]:
        return list(self.raw.get("allowed_builtin_tools", []))

    @property
    def allowed_mcp_tools(self) -> list[str]:
        return list(self.raw.get("allowed_mcp_tools", []))

    @property
    def disallowed_builtin_tools(self) -> list[str]:
        return list(self.raw.get("disallowed_builtin_tools", []))

    @property
    def allowed_tool_union(self) -> list[str]:
        return [*self.allowed_builtin_tools, *self.allowed_mcp_tools]

    def build_agent_claude_launch_spec(
        self,
        prompt_file: Path,
        mcp_config_file: Path,
        model: str = "sonnet",
        project_root: Path | None = None,
    ) -> dict[str, Any]:
        """
        Build a locked-down launch spec for the release `agent_claude` binary.

        This does not patch the binary. It constrains the session through CLI
        flags and an explicit MCP config.
        """
        knowledge_roots = list(self.raw.get("allowed_knowledge_roots", []))
        resolved_roots = [
            str((project_root / root).resolve()) if project_root else root
            for root in knowledge_roots
        ]
        add_dir_flags = " ".join(f'--add-dir "{root}"' for root in resolved_roots)
        return {
            "mode": "restricted_medical_insurance",
            "model": model,
            "mcp_config": str(mcp_config_file),
            "allowed_tools": self.allowed_tool_union,
            "disallowed_tools": self.disallowed_builtin_tools,
            "allowed_knowledge_roots": resolved_roots,
            "command_preview": [
                "powershell",
                "-NoProfile",
                "-Command",
                (
                    f"Get-Content -LiteralPath '{prompt_file}' -Raw | "
                    f"claude -p --bare --model {model} "
                    f"--permission-mode bypassPermissions "
                    f"--allowedTools \"{','.join(self.allowed_tool_union)}\" "
                    f"--disallowedTools \"{','.join(self.disallowed_builtin_tools)}\" "
                    f"--mcp-config \"{mcp_config_file}\" "
                    f"{add_dir_flags} "
                    f"--output-format stream-json"
                ),
            ],
            "notes_vi": [
                "Chi cho phep Read/Bash va MCP Neo4j da duoc cho phep.",
                "Khong cho Edit, SkillTool, Agent, WebFetch, WebSearch.",
                "Dung --bare de tranh auto-memory va plugin sync ngoai domain.",
            ],
        }
