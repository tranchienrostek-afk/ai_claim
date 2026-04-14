from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from dotenv import load_dotenv
from langchain_openai import AzureOpenAIEmbeddings, OpenAIEmbeddings
from sklearn.feature_extraction.text import TfidfVectorizer

from auto_review import strip_noise, to_skeleton
from chuan_hoa_dich_vu import normalize_service_name
from service_family_mapper import ServiceFamilyMapper


DATA_DIR = Path(__file__).parent
DEFAULT_CODEBOOK_PATH = DATA_DIR / "service_codebook.json"
DEFAULT_MAPPING_POLICY_PATH = DATA_DIR / "service_mapping_policy.json"
DEFAULT_PROTOCOL_RULES_PATH = DATA_DIR.parent / "05_reference" / "phac_do" / "tmh_protocol_text_rules.jsonl"
DEFAULT_PROTOCOL_LINKS_PATH = DATA_DIR.parent / "05_reference" / "phac_do" / "tmh_protocol_text_service_links.json"

LEXICAL_PRESELECT_K = 80
SEMANTIC_RERANK_K = 16


def load_env() -> None:
    for candidate in (DATA_DIR.parents[2] / ".env", DATA_DIR.parents[1] / ".env"):
        if candidate.exists():
            load_dotenv(candidate, override=False)


def build_embeddings_client(model_name: str = "text-embedding-3-large") -> Any | None:
    load_env()
    openai_api_key = os.getenv("OPENAI_API_KEY")
    if openai_api_key:
        return OpenAIEmbeddings(model=model_name, api_key=openai_api_key)

    azure_endpoint = os.getenv("AZURE_EMBEDDINGS_ENDPOINT") or os.getenv("AZURE_OPENAI_ENDPOINT")
    azure_api_key = os.getenv("AZURE_EMBEDDINGS_API_KEY") or os.getenv("AZURE_OPENAI_API_KEY")
    azure_api_version = os.getenv("AZURE_OPENAI_API_VERSION")
    azure_deployment = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT") or model_name
    if azure_endpoint and azure_api_key and azure_api_version:
        return AzureOpenAIEmbeddings(
            model=azure_deployment,
            azure_deployment=azure_deployment,
            azure_endpoint=azure_endpoint,
            api_key=azure_api_key,
            api_version=azure_api_version,
        )
    return None


def normalize_text(value: Any) -> str:
    cleaned, _meta = normalize_service_name(str(value or ""))
    return cleaned


def cosine_similarity(left: list[float], right: list[float]) -> float:
    left_vec = np.array(left, dtype=float)
    right_vec = np.array(right, dtype=float)
    left_norm = np.linalg.norm(left_vec)
    right_norm = np.linalg.norm(right_vec)
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0
    return float(np.dot(left_vec, right_vec) / (left_norm * right_norm))


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


@dataclass(frozen=True)
class HybridVariant:
    service_code: str
    canonical_name: str
    category_code: str
    category_name: str
    variant_text: str
    cleaned_name: str
    stripped_name: str
    skeleton: str
    source: str
    family_id: str | None


class HybridSemanticServiceTextMapper:
    def __init__(
        self,
        codebook_path: Path = DEFAULT_CODEBOOK_PATH,
        mapping_policy_path: Path = DEFAULT_MAPPING_POLICY_PATH,
        protocol_rules_path: Path = DEFAULT_PROTOCOL_RULES_PATH,
        protocol_links_path: Path = DEFAULT_PROTOCOL_LINKS_PATH,
        embedding_model: str = "text-embedding-3-large",
    ) -> None:
        self.codebook_path = codebook_path
        self.mapping_policy_path = mapping_policy_path
        self.protocol_rules_path = protocol_rules_path
        self.protocol_links_path = protocol_links_path
        self.embedding_model = embedding_model
        self.embedding_client = build_embeddings_client(embedding_model)
        self.embedding_cache: dict[str, list[float]] = {}
        self.family_mapper = ServiceFamilyMapper()
        self.mapping_policy = self._load_mapping_policy(mapping_policy_path)
        self.alias_supplements = self.mapping_policy.get("alias_supplements") or []
        self.family_only_aliases = self._build_family_only_aliases(self.mapping_policy.get("family_only_aliases") or [])

        self.variants: list[HybridVariant] = []
        self._load_codebook_variants()
        self._load_policy_aliases()
        self._load_protocol_aliases()

        self.cleaned_texts = [variant.cleaned_name for variant in self.variants]
        self.skeleton_texts = [variant.skeleton or variant.cleaned_name for variant in self.variants]
        self.word_vectorizer = TfidfVectorizer(analyzer="word", ngram_range=(1, 3), lowercase=False)
        self.char_vectorizer = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5), lowercase=False)
        self.word_matrix = self.word_vectorizer.fit_transform(self.cleaned_texts)
        self.char_matrix = self.char_vectorizer.fit_transform(self.skeleton_texts)

    def _load_mapping_policy(self, mapping_policy_path: Path) -> dict[str, Any]:
        if not mapping_policy_path.exists():
            return {}
        return json.loads(mapping_policy_path.read_text(encoding="utf-8"))

    def _family_id_for_variant(
        self,
        *,
        service_code: str,
        canonical_name: str,
        explicit_family_id: str | None = None,
    ) -> str | None:
        if explicit_family_id:
            return explicit_family_id
        for row in self.alias_supplements:
            if row.get("service_code") == service_code and row.get("family_id"):
                return str(row["family_id"])
        family_result = self.family_mapper.score_text(canonical_name, top_k=1)
        if family_result.get("mapping_status") in {"exact", "probable", "ambiguous"}:
            return str(family_result.get("family_id") or "") or None
        return None

    def _build_family_only_aliases(self, rows: list[dict[str, Any]]) -> dict[str, dict[str, str]]:
        alias_map: dict[str, dict[str, str]] = {}
        for row in rows:
            family_id = str(row.get("family_id") or "").strip()
            if not family_id:
                continue
            family_label = str(row.get("family_label") or family_id).strip()
            for alias in row.get("aliases") or []:
                cleaned = normalize_text(alias)
                if cleaned:
                    alias_map[cleaned] = {"family_id": family_id, "family_label": family_label}
        return alias_map

    def _add_variant(
        self,
        *,
        service_code: str,
        canonical_name: str,
        category_code: str,
        category_name: str,
        variant_text: str,
        source: str,
        family_id: str | None,
        seen: set[tuple[str, str]],
    ) -> None:
        cleaned = normalize_text(variant_text)
        if not cleaned:
            return
        key = (service_code, cleaned)
        if key in seen:
            return
        seen.add(key)
        stripped = strip_noise(cleaned)
        skeleton = to_skeleton(stripped or cleaned)
        self.variants.append(
            HybridVariant(
                service_code=service_code,
                canonical_name=canonical_name,
                category_code=category_code,
                category_name=category_name,
                variant_text=str(variant_text or "").strip(),
                cleaned_name=cleaned,
                stripped_name=stripped,
                skeleton=skeleton,
                source=source,
                family_id=family_id,
            )
        )

    def _load_codebook_variants(self) -> None:
        payload = json.loads(self.codebook_path.read_text(encoding="utf-8"))
        seen: set[tuple[str, str]] = set()
        for cluster in payload.get("codebook", []):
            service_code = str(cluster.get("service_code") or "")
            canonical_name = str(cluster.get("canonical_name") or "")
            category_code = str(cluster.get("category_code") or "")
            category_name = str(cluster.get("category_name") or "")
            family_id = self._family_id_for_variant(service_code=service_code, canonical_name=canonical_name)
            self._add_variant(
                service_code=service_code,
                canonical_name=canonical_name,
                category_code=category_code,
                category_name=category_name,
                variant_text=canonical_name,
                source="codebook_canonical",
                family_id=family_id,
                seen=seen,
            )
            for variant in cluster.get("variants", []):
                self._add_variant(
                    service_code=service_code,
                    canonical_name=canonical_name,
                    category_code=category_code,
                    category_name=category_name,
                    variant_text=variant.get("cleaned_name") or "",
                    source="codebook_variant",
                    family_id=family_id,
                    seen=seen,
                )

    def _load_policy_aliases(self) -> None:
        seen = {(item.service_code, item.cleaned_name) for item in self.variants}
        for row in self.alias_supplements:
            service_code = str(row.get("service_code") or "")
            if not service_code:
                continue
            family_id = self._family_id_for_variant(
                service_code=service_code,
                canonical_name=str(row.get("canonical_name") or ""),
                explicit_family_id=str(row.get("family_id") or "").strip() or None,
            )
            for alias in row.get("aliases") or []:
                self._add_variant(
                    service_code=service_code,
                    canonical_name=str(row.get("canonical_name") or alias),
                    category_code=str(row.get("category_code") or ""),
                    category_name=str(row.get("category_name") or ""),
                    variant_text=alias,
                    source="policy_alias",
                    family_id=family_id,
                    seen=seen,
                )

    def _load_protocol_aliases(self) -> None:
        seen = {(item.service_code, item.cleaned_name) for item in self.variants}
        if self.protocol_links_path.exists():
            payload = json.loads(self.protocol_links_path.read_text(encoding="utf-8"))
            for link in payload.get("links", []):
                service_code = str(link.get("service_code") or "")
                if not service_code:
                    continue
                canonical_name = str(link.get("service_name") or "")
                category_code = str(link.get("category_code") or "")
                category_name = str(link.get("category_name") or "")
                for source in link.get("sources") or []:
                    self._add_variant(
                        service_code=service_code,
                        canonical_name=canonical_name,
                        category_code=category_code,
                        category_name=category_name,
                        variant_text=source.get("item_name") or source.get("source_line") or "",
                        source="protocol_link",
                        family_id=self._family_id_for_variant(service_code=service_code, canonical_name=canonical_name),
                        seen=seen,
                    )

        if self.protocol_rules_path.exists():
            for row in load_jsonl(self.protocol_rules_path):
                if row.get("rule_type") != "service":
                    continue
                service_code = str(row.get("service_code") or "")
                if not service_code:
                    continue
                if str(row.get("mapping_resolution") or "") not in {"exact", "probable"}:
                    continue
                self._add_variant(
                    service_code=service_code,
                    canonical_name=str(row.get("service_name") or row.get("item_text") or ""),
                    category_code=str(row.get("category_code") or ""),
                    category_name=str(row.get("category_name") or ""),
                    variant_text=row.get("item_text") or row.get("source_line") or "",
                    source="protocol_rule",
                    family_id=self._family_id_for_variant(service_code=service_code, canonical_name=str(row.get("service_name") or row.get("item_text") or "")),
                    seen=seen,
                )

    def _embed_text(self, text: str) -> list[float]:
        if text in self.embedding_cache:
            return self.embedding_cache[text]
        if self.embedding_client is None:
            self.embedding_cache[text] = []
            return []
        vector = self.embedding_client.embed_query(text)
        self.embedding_cache[text] = vector
        return vector

    def score_text(self, raw_text: str, top_k: int = 3) -> dict[str, Any]:
        cleaned = normalize_text(raw_text)
        stripped = strip_noise(cleaned)
        skeleton = to_skeleton(stripped or cleaned)
        family_result = self.family_mapper.score_text(raw_text, top_k=1)
        query_family_id = family_result.get("family_id")
        query_family_status = str(family_result.get("mapping_status") or "unknown")

        query_word = self.word_vectorizer.transform([cleaned])
        query_char = self.char_vectorizer.transform([skeleton or cleaned])

        word_scores = (self.word_matrix @ query_word.T).toarray().ravel()
        char_scores = (self.char_matrix @ query_char.T).toarray().ravel()
        lexical_scores = 0.45 * word_scores + 0.55 * char_scores

        if cleaned:
            for idx, variant in enumerate(self.variants):
                if variant.cleaned_name == cleaned:
                    lexical_scores[idx] = max(lexical_scores[idx], 1.0)
                elif stripped and variant.stripped_name == stripped:
                    lexical_scores[idx] = max(lexical_scores[idx], 0.96)

        preselect_ids = np.argsort(-lexical_scores)[:LEXICAL_PRESELECT_K]
        query_embedding = self._embed_text(cleaned) if cleaned else []

        best_by_code: dict[str, dict[str, Any]] = {}
        semantic_used = bool(query_embedding)

        for idx in preselect_ids:
            variant = self.variants[int(idx)]
            lexical = float(lexical_scores[int(idx)])
            semantic = 0.0
            if semantic_used and idx in preselect_ids[:SEMANTIC_RERANK_K]:
                variant_embedding = self._embed_text(variant.cleaned_name or variant.variant_text)
                if variant_embedding:
                    semantic = cosine_similarity(query_embedding, variant_embedding)

            exact_clean = variant.cleaned_name == cleaned
            exact_stripped = bool(stripped and variant.stripped_name == stripped)
            final_score = (0.72 * lexical) + (0.28 * semantic)
            reasons: list[str] = []
            if query_family_id and variant.family_id:
                if query_family_id == variant.family_id:
                    final_score += 0.06
                    reasons.append("same_service_family")
                elif query_family_status in {"exact", "probable"}:
                    final_score -= 0.08
                    reasons.append("penalty_family_conflict")
            if exact_clean:
                final_score = max(final_score, 0.995)
            elif exact_stripped:
                final_score = max(final_score, 0.96)

            if exact_clean:
                reasons.append("exact_cleaned_match")
            if exact_stripped:
                reasons.append("exact_after_noise_strip")
            if variant.source != "codebook_variant":
                reasons.append(f"alias_source:{variant.source}")
            if semantic >= 0.84:
                reasons.append("semantic_similarity_high")
            elif semantic >= 0.75:
                reasons.append("semantic_similarity_good")
            if lexical >= 0.9:
                reasons.append("lexical_similarity_high")

            current = best_by_code.get(variant.service_code)
            if current is None or final_score > current["score"]:
                best_by_code[variant.service_code] = {
                    "service_code": variant.service_code,
                    "canonical_name": variant.canonical_name,
                    "category_code": variant.category_code,
                    "category_name": variant.category_name,
                    "matched_variant": variant.variant_text or variant.cleaned_name,
                    "score": final_score,
                    "lexical_score": lexical,
                    "semantic_score": semantic,
                    "reasons": reasons,
                    "source": variant.source,
                    "family_id": variant.family_id,
                }

        suggestions = sorted(best_by_code.values(), key=lambda item: (-item["score"], item["service_code"]))[:top_k]
        for index, item in enumerate(suggestions):
            second = suggestions[index + 1]["score"] if index + 1 < len(suggestions) else 0.0
            gap = item["score"] - second
            if "exact_cleaned_match" in item["reasons"] or "exact_after_noise_strip" in item["reasons"]:
                confidence = "HIGH"
            elif item["score"] >= 0.9 and gap >= 0.05:
                confidence = "HIGH"
            elif item["score"] >= 0.84 and gap >= 0.03:
                confidence = "MEDIUM"
            elif item["score"] >= 0.76:
                confidence = "LOW"
            else:
                confidence = "REVIEW"
            item["confidence"] = confidence

        family_only = self.family_only_aliases.get(cleaned)
        mapping_resolution = "coded" if suggestions else ("family_only" if family_only else "unknown")
        if suggestions and suggestions[0]["confidence"] == "REVIEW" and family_only:
            mapping_resolution = "family_only"

        return {
            "input_text": raw_text,
            "cleaned_text": cleaned,
            "family_hint_id": query_family_id,
            "family_hint_status": query_family_status,
            "mapping_resolution": mapping_resolution,
            "family_only_hint": family_only,
            "embedding_enabled": semantic_used,
            "suggestions": [
                {
                    "service_code": item["service_code"],
                    "canonical_name": item["canonical_name"],
                    "category_code": item["category_code"],
                    "category_name": item["category_name"],
                    "matched_variant": item["matched_variant"],
                    "score": round(item["score"] * 100, 2),
                    "confidence": item["confidence"],
                    "reasons": item["reasons"],
                    "conflicts": [],
                    "family_id": item.get("family_id"),
                    "metrics": {
                        "lexical_score": round(item["lexical_score"] * 100, 2),
                        "semantic_score": round(item["semantic_score"] * 100, 2),
                    },
                }
                for item in suggestions
            ],
        }


def main() -> None:
    mapper = HybridSemanticServiceTextMapper()
    sample = mapper.score_text("Khám chuyên khoa Tai Mũi Họng", top_k=5)
    print(json.dumps(sample, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
