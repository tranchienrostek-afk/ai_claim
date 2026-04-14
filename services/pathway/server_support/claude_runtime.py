from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from server_support.claude_workspace_memory import load_runtime_memory_text, runtime_memory_status


DEFAULT_MODEL = os.getenv("CLAUDE_DUET_MODEL", "claude-opus-4-6")
NOTEBOOKLM_DIR = Path(__file__).resolve().parent.parent
PROJECT_MAP_PATH = NOTEBOOKLM_DIR / "CLAUDE_PROJECT_MAP.md"
PROJECT_PLUGIN_DIR = NOTEBOOKLM_DIR / "claude_marketplace" / "plugins" / "pathway-intelligence"
_PROJECT_MAP_CACHE: Dict[str, Any] = {"mtime": None, "text": None}


def env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return max(0, int(raw_value))
    except ValueError:
        return default


PROJECT_MAP_MAX_CHARS = env_int("CLAUDE_DUET_PROJECT_MAP_MAX_CHARS", 12000)


class ClaudeCliError(RuntimeError):
    pass


class ClaudeCliUnavailableError(ClaudeCliError):
    pass


def _truncate_text(text: str, max_chars: int) -> str:
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    if "\n" in truncated:
        truncated = truncated.rsplit("\n", 1)[0]
    return truncated.rstrip() + "\n\n[Truncated project map for prompt budget]"


def load_project_map_text() -> Optional[str]:
    try:
        stat = PROJECT_MAP_PATH.stat()
    except FileNotFoundError:
        _PROJECT_MAP_CACHE["mtime"] = None
        _PROJECT_MAP_CACHE["text"] = None
        return None

    if _PROJECT_MAP_CACHE.get("mtime") == stat.st_mtime and _PROJECT_MAP_CACHE.get("text") is not None:
        return _PROJECT_MAP_CACHE["text"]

    text = PROJECT_MAP_PATH.read_text(encoding="utf-8").strip()
    text = _truncate_text(text, PROJECT_MAP_MAX_CHARS)
    _PROJECT_MAP_CACHE["mtime"] = stat.st_mtime
    _PROJECT_MAP_CACHE["text"] = text
    return text


def project_context_status() -> Dict[str, Any]:
    text = load_project_map_text()
    return {
        "available": bool(text),
        "path": str(PROJECT_MAP_PATH),
        "chars": len(text or ""),
        "max_chars": PROJECT_MAP_MAX_CHARS,
    }


def project_plugin_status() -> Dict[str, Any]:
    return {
        "available": PROJECT_PLUGIN_DIR.is_dir(),
        "path": str(PROJECT_PLUGIN_DIR),
    }


def compose_system_prompt(system_prompt: str) -> str:
    project_map = load_project_map_text()
    runtime_memory = load_runtime_memory_text()
    if not project_map and not runtime_memory:
        return system_prompt

    sections: List[str] = []
    if project_map:
        sections.extend(
            [
                "Curated Pathway project memory follows. Treat it as trusted workspace context for architecture and workflow reasoning.",
                project_map,
            ]
        )
    if runtime_memory:
        sections.extend(
            [
                "Auto-generated Pathway runtime memory follows. Treat it as recent operational context synthesized from local bridge interactions and operator feedback.",
                runtime_memory,
            ]
        )
    sections.extend(["Current turn role instructions:", system_prompt])
    return "\n\n".join(section for section in sections if section)


class ClaudeCliRuntime:
    def __init__(self, binary: str = "claude") -> None:
        self.binary = os.getenv("CLAUDE_CLI_PATH", binary)

    def _resolved_binary(self) -> Optional[str]:
        return shutil.which(self.binary)

    def is_available(self) -> bool:
        return self._resolved_binary() is not None

    def _env(self) -> Dict[str, str]:
        env = os.environ.copy()
        env.setdefault("CLAUDE_CODE_DISABLE_AUTOUPDATER", "1")
        env.setdefault("HOME", "/root")
        return env

    def _run_simple(self, args: List[str], timeout: int = 30) -> str:
        process = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=self._env(),
            cwd=str(NOTEBOOKLM_DIR),
        )
        if process.returncode != 0:
            message = (process.stderr or process.stdout or "").strip()
            raise ClaudeCliError(message or f"Claude exited with code {process.returncode}")
        return (process.stdout or "").strip()

    def status(self) -> Dict[str, Any]:
        binary = self._resolved_binary()
        payload: Dict[str, Any] = {
            "available": binary is not None,
            "binary": binary or self.binary,
            "default_model": DEFAULT_MODEL,
            "project_context": project_context_status(),
            "runtime_memory": runtime_memory_status(),
            "project_plugin": project_plugin_status(),
        }
        if binary is None:
            return payload

        try:
            payload["version"] = self._run_simple([binary, "--version"])
        except ClaudeCliError as exc:
            payload["version_error"] = str(exc)

        try:
            import json

            raw_auth = self._run_simple([binary, "auth", "status"])
            payload["auth"] = json.loads(raw_auth)
        except Exception as exc:
            payload["auth_error"] = str(exc)

        return payload

    def invoke(
        self,
        prompt: str,
        *,
        system_prompt: str,
        model: Optional[str] = None,
        timeout: int = 180,
        tools: Optional[str] = "",
    ) -> Dict[str, Any]:
        binary = self._resolved_binary()
        if binary is None:
            raise ClaudeCliUnavailableError(
                "Khong tim thay lenh 'claude' trong container API. Can rebuild Docker image truoc."
            )

        effective_system_prompt = compose_system_prompt(system_prompt)
        command = [
            binary,
            "-p",
            "--output-format",
            "text",
            "--no-session-persistence",
            "--permission-mode",
            "default",
            "--system-prompt",
            effective_system_prompt,
        ]
        if tools is not None:
            command.extend(["--tools", tools])
        if model:
            command.extend(["--model", model])
        if PROJECT_PLUGIN_DIR.is_dir():
            command.extend(["--plugin-dir", str(PROJECT_PLUGIN_DIR)])
        command.append(prompt)

        started = time.perf_counter()
        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=self._env(),
            cwd=str(NOTEBOOKLM_DIR),
        )
        duration_ms = round((time.perf_counter() - started) * 1000, 1)

        if process.returncode != 0:
            message = (process.stderr or process.stdout or "").strip()
            raise ClaudeCliError(message or f"Claude exited with code {process.returncode}")

        content = (process.stdout or "").strip()
        if not content:
            raise ClaudeCliError("Claude khong tra ve noi dung.")

        return {"content": content, "duration_ms": duration_ms}
