import uuid
from typing import Dict, List, Optional, Tuple


class SessionStore:
    def __init__(self, max_turns: int = 20):
        self.max_turns = max_turns
        self._sessions: Dict[str, List[dict]] = {}

    def get_or_create(self, session_id: Optional[str]) -> Tuple[str, List[dict]]:
        if not session_id:
            session_id = str(uuid.uuid4())[:8]
        if session_id not in self._sessions:
            self._sessions[session_id] = []
        return session_id, self._sessions[session_id]

    def append(self, session_id: str, role: str, content: str) -> None:
        if session_id not in self._sessions:
            self._sessions[session_id] = []
        self._sessions[session_id].append({"role": role, "content": content})
        max_messages = self.max_turns * 2
        if len(self._sessions[session_id]) > max_messages:
            self._sessions[session_id] = self._sessions[session_id][-max_messages:]
