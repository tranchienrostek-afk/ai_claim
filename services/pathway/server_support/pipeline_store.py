import io
import json
import queue
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


class PipelineRunStore:
    def __init__(self, runs_dir: Path):
        self.runs_dir = runs_dir
        self.runs_dir.mkdir(exist_ok=True)
        self._runs: Dict[str, dict] = {}
        self._listeners: Dict[str, List[queue.Queue]] = {}
        self._lock = threading.RLock()

    def load_from_disk(self) -> None:
        loaded: Dict[str, dict] = {}
        for run_file in self.runs_dir.glob("*.json"):
            try:
                with open(run_file, "r", encoding="utf-8") as handle:
                    data = json.load(handle)
                    loaded[data.get("id", run_file.stem)] = data
            except (OSError, json.JSONDecodeError):
                continue
        with self._lock:
            self._runs.update(loaded)

    def save(self, run_id: str) -> None:
        with self._lock:
            if run_id not in self._runs:
                return
            payload = dict(self._runs[run_id])
        with open(self.runs_dir / f"{run_id}.json", "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)

    def create_run(self, run_id: str, payload: dict) -> dict:
        with self._lock:
            self._runs[run_id] = dict(payload)
        self.save(run_id)
        return payload

    def has_run(self, run_id: str) -> bool:
        with self._lock:
            return run_id in self._runs

    def get_run(self, run_id: str) -> Optional[dict]:
        with self._lock:
            return self._runs.get(run_id)

    def update_run(self, run_id: str, **updates) -> Optional[dict]:
        with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                return None
            run.update(updates)
            updated = dict(run)
        self.save(run_id)
        return updated

    def mark_completed(self, run_id: str, result: Optional[dict] = None) -> None:
        updates = {"status": "completed"}
        if result is not None:
            updates["result"] = result
        self.update_run(run_id, **updates)

    def mark_error(self, run_id: str, error: str) -> None:
        self.update_run(run_id, status="error", error=error)

    def iter_runs(self) -> List[dict]:
        with self._lock:
            return list(self._runs.values())

    def get_logs(self, run_id: str) -> List[dict]:
        with self._lock:
            run = self._runs.get(run_id, {})
            return list(run.get("logs", []))

    def add_listener(self, run_id: str) -> queue.Queue:
        listener = queue.Queue()
        with self._lock:
            self._listeners.setdefault(run_id, []).append(listener)
        return listener

    def remove_listener(self, run_id: str, listener: queue.Queue) -> None:
        with self._lock:
            if run_id not in self._listeners:
                return
            if listener in self._listeners[run_id]:
                self._listeners[run_id].remove(listener)
            if not self._listeners[run_id]:
                self._listeners.pop(run_id, None)

    def record_log(self, run_id: str, text: str) -> None:
        message = {
            "type": "log",
            "message": text,
            "timestamp": datetime.now().isoformat(),
        }
        listeners: List[queue.Queue] = []
        with self._lock:
            run = self._runs.get(run_id)
            if run is not None:
                run.setdefault("logs", []).append(message)
            listeners = list(self._listeners.get(run_id, []))
        for listener in listeners:
            listener.put(message)


class PipelineLogCapture:
    encoding = "utf-8"
    errors = "replace"

    def __init__(self, run_id: str, original_stdout, run_store: PipelineRunStore):
        self.run_id = run_id
        self.original = original_stdout
        self.run_store = run_store

    @property
    def buffer(self):
        return getattr(self.original, "buffer", io.BytesIO())

    def write(self, text):
        if not isinstance(text, str):
            text = str(text)
        try:
            self.original.write(text)
        except (UnicodeEncodeError, OSError):
            self.original.write(text.encode("ascii", "replace").decode("ascii"))
        if text.strip():
            self.run_store.record_log(self.run_id, text.strip())
        return len(text)

    def flush(self):
        self.original.flush()

    def writable(self):
        return True

    def readable(self):
        return False

    def fileno(self):
        return self.original.fileno()
