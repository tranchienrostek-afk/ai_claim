from __future__ import annotations

import json
import os
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from langchain_community.retrievers import BM25Retriever
from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document
from langchain_openai import AzureOpenAIEmbeddings, OpenAIEmbeddings


PROJECT_DIR = Path(__file__).parent.parent
NOTEBOOKLM_DIR = Path(__file__).resolve().parents[2]
ROOT_DIR = Path(__file__).resolve().parents[3]

RAG_DIR = PROJECT_DIR / "05_reference" / "phac_do" / "protocol_pdf_semantic_rag"
CHILD_DOCS_PATH = RAG_DIR / "child_chunks.jsonl"
FAISS_INDEX_DIR = RAG_DIR / "faiss_index"
DEFAULT_EMBEDDING_MODEL = "text-embedding-3-large"

SERVICE_STOPWORDS = {
    "",
    "can",
    "lam",
    "sang",
    "xet",
    "nghiem",
    "chup",
    "do",
    "test",
    "nhanh",
    "mau",
    "theo",
    "doi",
    "dinh",
    "luong",
    "tong",
    "phan",
    "tich",
}

DISEASE_STOPWORDS = {
    "",
    "benh",
    "benhly",
    "benhli",
    "hoi",
    "chung",
    "theo",
    "doi",
    "td",
    "cap",
    "man",
    "tinh",
    "khong",
    "xac",
    "dinh",
    "day",
    "than",
    "kinh",
    "ngoai",
    "bien",
    "mat",
    "liet",
}


def as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def clean_phrase(text: str) -> str:
    phrase = as_text(text)
    phrase = re.sub(r"\s+", " ", phrase)
    return phrase.strip(" .;,:-")


def normalize_phrase(text: str) -> str:
    phrase = clean_phrase(text).lower().replace("đ", "d")
    phrase = unicodedata.normalize("NFKD", phrase)
    phrase = "".join(char for char in phrase if not unicodedata.combining(char))
    phrase = re.sub(r"[^a-z0-9 ]+", " ", phrase)
    phrase = re.sub(r"\s+", " ", phrase).strip()
    return phrase


def tokenize_service(text: str) -> set[str]:
    tokens: set[str] = set()
    for token in normalize_phrase(text).split():
        if token in SERVICE_STOPWORDS or len(token) <= 1:
            continue
        tokens.add(token)
    return tokens


def tokenize_disease(text: str) -> set[str]:
    tokens: set[str] = set()
    for token in normalize_phrase(text).split():
        if token in DISEASE_STOPWORDS:
            continue
        if len(token) <= 2 and not token.isdigit():
            continue
        tokens.add(token)
    return tokens


def reciprocal_rank_fusion(rank: int) -> float:
    return 1.0 / (60 + rank)


def load_env() -> None:
    for candidate in (ROOT_DIR / ".env", NOTEBOOKLM_DIR / ".env"):
        if candidate.exists():
            load_dotenv(candidate, override=False)


def build_embeddings_client() -> Any | None:
    load_env()

    openai_api_key = os.getenv("OPENAI_API_KEY")
    if openai_api_key:
        return OpenAIEmbeddings(model=DEFAULT_EMBEDDING_MODEL, api_key=openai_api_key)

    azure_endpoint = os.getenv("AZURE_EMBEDDINGS_ENDPOINT") or os.getenv("AZURE_OPENAI_ENDPOINT")
    azure_api_key = os.getenv("AZURE_EMBEDDINGS_API_KEY") or os.getenv("AZURE_OPENAI_API_KEY")
    azure_api_version = os.getenv("AZURE_OPENAI_API_VERSION")
    azure_deployment = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT") or DEFAULT_EMBEDDING_MODEL
    if azure_endpoint and azure_api_key and azure_api_version:
        return AzureOpenAIEmbeddings(
            model=azure_deployment,
            azure_deployment=azure_deployment,
            azure_endpoint=azure_endpoint,
            api_key=azure_api_key,
            api_version=azure_api_version,
        )

    return None


def load_child_documents(path: Path) -> list[Document]:
    if not path.exists():
        return []
    documents: list[Document] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            documents.append(
                Document(
                    page_content=as_text(row.get("page_content")),
                    metadata=row.get("metadata") or {},
                )
            )
    return documents


@dataclass
class ProtocolSemanticAssessment:
    supported: bool
    support_level: str
    best_score: float
    parent_hits: list[dict[str, Any]]
    retrieval_modes: list[str]


class ProtocolSemanticSupportEngine:
    def __init__(self, child_docs_path: Path = CHILD_DOCS_PATH, faiss_index_dir: Path = FAISS_INDEX_DIR) -> None:
        self.child_docs = load_child_documents(child_docs_path)
        self.bm25 = BM25Retriever.from_documents(self.child_docs) if self.child_docs else None
        if self.bm25 is not None:
            self.bm25.k = 6
        self.faiss_index_dir = faiss_index_dir
        self._semantic_vectorstore: FAISS | None = None
        self._semantic_ready = False
        self._cache: dict[tuple[str, str, str], ProtocolSemanticAssessment] = {}

    def _load_semantic_index(self) -> None:
        if self._semantic_ready:
            return
        self._semantic_ready = True
        if not self.faiss_index_dir.exists():
            return
        embeddings = build_embeddings_client()
        if embeddings is None:
            return
        self._semantic_vectorstore = FAISS.load_local(
            str(self.faiss_index_dir),
            embeddings,
            allow_dangerous_deserialization=True,
        )

    def _run_bm25(self, query: str) -> list[dict[str, Any]]:
        if self.bm25 is None:
            return []
        docs = self.bm25.invoke(query)
        rows: list[dict[str, Any]] = []
        for rank, doc in enumerate(docs, start=1):
            rows.append(
                {
                    "retrieval_mode": "bm25",
                    "rank": rank,
                    "rrf_score": reciprocal_rank_fusion(rank),
                    "document": doc,
                }
            )
        return rows

    def _run_semantic(self, query: str) -> list[dict[str, Any]]:
        self._load_semantic_index()
        if self._semantic_vectorstore is None:
            return []
        results = self._semantic_vectorstore.similarity_search_with_score(query, k=6)
        rows: list[dict[str, Any]] = []
        for rank, (doc, _score) in enumerate(results, start=1):
            rows.append(
                {
                    "retrieval_mode": "semantic",
                    "rank": rank,
                    "rrf_score": reciprocal_rank_fusion(rank),
                    "document": doc,
                }
            )
        return rows

    @staticmethod
    def _score_parent_hit(
        doc: Document,
        icd_code: str,
        diagnosis_text: str,
        service_name: str,
        service_info: dict[str, Any],
        base_score: float,
    ) -> tuple[float, dict[str, Any]]:
        metadata = doc.metadata or {}
        page_content = as_text(doc.page_content)
        normalized_content = normalize_phrase(page_content)
        service_text = as_text(service_info.get("canonical_name")) or service_name
        service_norm = normalize_phrase(service_text)
        service_tokens = tokenize_service(service_text)
        content_tokens = set(normalized_content.split())
        diagnosis_tokens = tokenize_disease(diagnosis_text)
        disease_label = as_text(metadata.get("disease_name")) or as_text(metadata.get("disease_title"))
        disease_tokens = tokenize_disease(disease_label)

        service_overlap = len(service_tokens & content_tokens) / max(len(service_tokens), 1) if service_tokens else 0.0
        exact_service_mention = bool(service_norm and service_norm in normalized_content)
        icd_match = bool(icd_code and as_text(metadata.get("icd10")) == icd_code)
        shared_disease_tokens = disease_tokens & diagnosis_tokens
        disease_overlap = len(shared_disease_tokens) / max(len(disease_tokens), 1) if disease_tokens else 0.0
        disease_match = len(shared_disease_tokens) >= 2 and disease_overlap >= 0.34
        section_type = as_text(metadata.get("section_type"))
        paraclinical_match = section_type == "paraclinical"

        score = base_score
        if paraclinical_match:
            score += 0.04
        if icd_match:
            score += 0.05
        if disease_match:
            score += 0.05
        if exact_service_mention:
            score += 0.06
        score += 0.05 * service_overlap

        detail = {
            "parent_id": as_text(metadata.get("parent_id")),
            "disease_title": as_text(metadata.get("disease_title")),
            "disease_name": as_text(metadata.get("disease_name")),
            "icd10": as_text(metadata.get("icd10")),
            "section_title": as_text(metadata.get("section_title")),
            "section_type": section_type,
            "source_file": as_text(metadata.get("source_file")),
            "page_numbers": metadata.get("page_numbers") or [],
            "child_id": as_text(metadata.get("child_id")),
            "snippet": clean_phrase(page_content)[:360],
            "icd_match": icd_match,
            "disease_match": disease_match,
            "paraclinical_match": paraclinical_match,
            "exact_service_mention": exact_service_mention,
            "service_overlap": round(service_overlap, 4),
            "disease_overlap": round(disease_overlap, 4),
        }
        return round(score, 4), detail

    def assess(self, service_name: str, service_info: dict[str, Any], icd_code: str, diagnosis_text: str) -> ProtocolSemanticAssessment:
        cache_key = (
            normalize_phrase(as_text(service_info.get("canonical_name")) or service_name),
            normalize_phrase(icd_code),
            normalize_phrase(diagnosis_text)[:120],
        )
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        if not self.child_docs:
            result = ProtocolSemanticAssessment(False, "none", 0.0, [], [])
            self._cache[cache_key] = result
            return result

        query_terms = [
            as_text(icd_code),
            clean_phrase(diagnosis_text)[:180],
            as_text(service_info.get("canonical_name")) or service_name,
            "protocol paraclinical can lam sang",
        ]
        query = " ".join(part for part in query_terms if part)
        raw_hits = self._run_bm25(query) + self._run_semantic(query)

        grouped: dict[str, dict[str, Any]] = {}
        retrieval_modes: set[str] = set()
        for row in raw_hits:
            retrieval_modes.add(as_text(row.get("retrieval_mode")))
            doc = row.get("document")
            if not isinstance(doc, Document):
                continue
            score, detail = self._score_parent_hit(
                doc,
                icd_code,
                diagnosis_text,
                service_name,
                service_info,
                float(row.get("rrf_score", 0.0)),
            )
            parent_id = detail["parent_id"]
            if not parent_id:
                continue
            bucket = grouped.setdefault(
                parent_id,
                {
                    "parent_id": parent_id,
                    "disease_title": detail["disease_title"],
                    "disease_name": detail["disease_name"],
                    "icd10": detail["icd10"],
                    "section_title": detail["section_title"],
                    "section_type": detail["section_type"],
                    "source_file": detail["source_file"],
                    "page_numbers": detail["page_numbers"],
                    "composite_score": 0.0,
                    "service_overlap": 0.0,
                    "icd_match": False,
                    "disease_match": False,
                    "exact_service_mention": False,
                    "support_hits": [],
                },
            )
            bucket["composite_score"] = max(bucket["composite_score"], score)
            bucket["service_overlap"] = max(bucket["service_overlap"], detail["service_overlap"])
            bucket["icd_match"] = bucket["icd_match"] or detail["icd_match"]
            bucket["disease_match"] = bucket["disease_match"] or detail["disease_match"]
            bucket["exact_service_mention"] = bucket["exact_service_mention"] or detail["exact_service_mention"]
            bucket["support_hits"].append(
                {
                    "retrieval_mode": row.get("retrieval_mode"),
                    "rank": row.get("rank"),
                    "child_id": detail["child_id"],
                    "snippet": detail["snippet"],
                }
            )

        parent_hits = sorted(grouped.values(), key=lambda item: item["composite_score"], reverse=True)[:3]
        best_score = float(parent_hits[0]["composite_score"]) if parent_hits else 0.0

        if parent_hits and parent_hits[0]["section_type"] == "paraclinical" and (
            parent_hits[0]["icd_match"] or parent_hits[0]["disease_match"]
        ) and (
            parent_hits[0]["exact_service_mention"] or parent_hits[0]["service_overlap"] >= 0.4
        ):
            support_level = "strong" if best_score >= 0.11 else "moderate"
            supported = best_score >= 0.085
        else:
            support_level = "none"
            supported = False

        result = ProtocolSemanticAssessment(
            supported=supported,
            support_level=support_level,
            best_score=round(best_score, 4),
            parent_hits=parent_hits,
            retrieval_modes=sorted(mode for mode in retrieval_modes if mode),
        )
        self._cache[cache_key] = result
        return result
