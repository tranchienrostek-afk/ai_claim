from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Any


PROJECT_DIR = Path(__file__).parent.parent
DEFAULT_POLICY_PATH = PROJECT_DIR / "05_reference" / "signs" / "sign_phrase_decomposition_policy.json"
MOJIBAKE_HINTS = ("Ãƒ", "Ã„", "Ã…", "Ã†", "Ã‚", "Ã")


def repair_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if any(hint in text for hint in MOJIBAKE_HINTS):
        try:
            repaired = text.encode("latin1").decode("utf-8")
            if repaired:
                return repaired
        except Exception:
            pass
    return text


def ascii_fold(value: Any) -> str:
    lowered = repair_text(value).lower()
    lowered = lowered.replace("đ", "d").replace("Đ", "d")
    normalized = unicodedata.normalize("NFD", lowered)
    stripped = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    stripped = re.sub(r"[^a-z0-9 ]+", " ", stripped)
    return re.sub(r"\s+", " ", stripped).strip()


class SignPhraseDecomposer:
    def __init__(self, policy_path: Path = DEFAULT_POLICY_PATH) -> None:
        payload = json.loads(policy_path.read_text(encoding="utf-8"))
        self.split_patterns = [re.compile(pattern, flags=re.IGNORECASE) for pattern in payload.get("split_regexes") or []]
        self.drop_patterns = [re.compile(pattern, flags=re.IGNORECASE) for pattern in payload.get("drop_regexes") or []]
        self.filler_terms = [ascii_fold(item) for item in payload.get("filler_terms") or []]
        self.stop_tokens = set(ascii_fold(item) for item in payload.get("stop_tokens") or [])
        self.min_ngram_tokens = max(2, int(payload.get("min_ngram_tokens") or 2))
        self.max_ngram_tokens = max(self.min_ngram_tokens, int(payload.get("max_ngram_tokens") or 5))
        self.max_fragments = max(8, int(payload.get("max_fragments") or 24))

    def _clean_text(self, raw_text: Any) -> str:
        text = repair_text(raw_text)
        for pattern in self.drop_patterns:
            text = pattern.sub(" ", text)
        folded = ascii_fold(text)
        for filler in self.filler_terms:
            if filler:
                folded = re.sub(rf"(?<!\w){re.escape(filler)}(?!\w)", " ", folded)
        return re.sub(r"\s+", " ", folded).strip()

    def _split_fragments(self, cleaned_text: str) -> list[str]:
        fragments = [cleaned_text]
        for pattern in self.split_patterns:
            next_fragments: list[str] = []
            for fragment in fragments:
                next_fragments.extend(part.strip() for part in pattern.split(fragment) if part.strip())
            fragments = next_fragments or fragments
        return fragments

    def _ngram_fragments(self, fragment: str) -> list[str]:
        tokens = [token for token in fragment.split() if token and token not in self.stop_tokens]
        ngrams: list[str] = []
        for width in range(self.min_ngram_tokens, min(self.max_ngram_tokens, len(tokens)) + 1):
            for start in range(0, len(tokens) - width + 1):
                ngram = " ".join(tokens[start : start + width]).strip()
                if ngram:
                    ngrams.append(ngram)
        return ngrams

    def decompose(self, raw_text: Any) -> list[str]:
        cleaned = self._clean_text(raw_text)
        if not cleaned:
            return []

        candidates: list[str] = [cleaned]
        for fragment in self._split_fragments(cleaned):
            if fragment:
                candidates.append(fragment)
                candidates.extend(self._ngram_fragments(fragment))

        seen: set[str] = set()
        ordered: list[str] = []
        for item in sorted(candidates, key=lambda value: (-len(value.split()), -len(value), value)):
            normalized = re.sub(r"\s+", " ", item).strip()
            if len(normalized.split()) < self.min_ngram_tokens and normalized != cleaned:
                continue
            if normalized and normalized not in seen:
                seen.add(normalized)
                ordered.append(normalized)
            if len(ordered) >= self.max_fragments:
                break
        return ordered
