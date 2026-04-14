"""
Batch text-to-Neo4j ingestion for protocol text files.

Text is treated as a first-class knowledge surface, not a temporary import aid.
The strengthened default path is:

    text file -> document profile -> ontology_v2 ingest -> structured Neo4j graph

Every run also writes a machine-readable manifest and a batch report so the
source text, graph contract, and ingest result stay auditable for downstream
reasoning.

Usage:
    cd notebooklm
    python batch_text_to_graph.py --dry-run
    python batch_text_to_graph.py --folder "Phac do tai mui hong" --dry-run
    python batch_text_to_graph.py --only "Benh Meniere" --skip-existing
    python batch_text_to_graph.py --engine universal --include-seeded --only "J03"
"""

from __future__ import annotations

import io
import hashlib
import json
import os
import sys
import time
import traceback
import unicodedata
from datetime import datetime
from pathlib import Path

from neo4j import GraphDatabase
from openai import AzureOpenAI

from ontology_v2_ingest import OntologyV2Ingest
from runtime_env import load_notebooklm_env
from server_support.paths import INGEST_CONFIGS_DIR, RUNS_DIR, ensure_pathway_data_layout
from universal_ingest import (
    DocumentAnalyzer,
    DocumentProfile,
    PipelineConfigurator,
    UniversalIngest,
    _slugify,
)


if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")


load_notebooklm_env()


NOTEBOOKLM_DIR = Path(__file__).resolve().parent
REPO_ROOT = NOTEBOOKLM_DIR.parent.parent
EXTRACTED_DIR = REPO_ROOT / "roadmap_master_data" / "Extracted_Protocols"
CONFIG_DIR = INGEST_CONFIGS_DIR / "text_to_neo4j"
RUN_ROOT_DIR = RUNS_DIR / "text_to_neo4j"
MANIFEST_DIR = RUN_ROOT_DIR / "manifests"
REPORT_DIR = RUN_ROOT_DIR / "reports"

DEFAULT_ENGINE = "ontology_v2"
DEFAULT_NAMESPACE = "ontology_v2"


SEEDED_TOP10_FILTER_TAGS = {
    "phac do tai mui hong/viem mui hong cap tinh.txt": {"j00", "j06"},
    "phac do ho hap/viem phe quan cap.txt": {"j20"},
    "phac do tai mui hong/viem hong cap tinh.txt": {"j02"},
    "phac do tai mui hong/viem mui xoang cap tinh.txt": {"j01"},
    "phac do tai mui hong/viem amidan cap va man tinh.txt": {"j03"},
    "phac do tai mui hong/viem mui xoang di ung.txt": {"j30"},
    "phac do ho hap/viem phoi mac phai o cong dong.txt": {"j18"},
    "phac do tai mui hong/viem thanh quan cap tinh.txt": {"j04"},
    "phac do hen phe quan/hen phe quan (nguoi lon va tre em >=12 tuoi).txt": {"j45"},
}


FOLDER_DOMAIN_HINTS = {
    "phac do hen phe quan": "Internal_Medicine",
    "phac do ho hap": "Internal_Medicine",
    "phac do tai mui hong": "Internal_Medicine",
    "phac do viem phoi": "Internal_Medicine",
}


def normalize_ascii(value: str) -> str:
    """Normalize text for fuzzy matching and seeded-file lookup."""
    normalized = (
        value.replace("≥", ">=")
        .replace("≤", "<=")
        .replace("–", "-")
        .replace("—", "-")
        .replace("’", "'")
        .replace("đ", "d")
        .replace("Đ", "D")
    )
    decomposed = unicodedata.normalize("NFKD", normalized)
    stripped = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    lowered = stripped.lower()
    return " ".join(lowered.replace("\\", "/").split())


def normalize_relpath(path: Path) -> str:
    return normalize_ascii(path.as_posix())


def looks_like_auxiliary_text(path: Path) -> bool:
    name = normalize_ascii(path.name)
    return name == "_full_text.txt"


def default_domain_for_path(path: Path) -> str:
    folder_key = normalize_ascii(path.parent.name)
    return FOLDER_DOMAIN_HINTS.get(folder_key, "Internal_Medicine")


def discover_text_files(
    include_seeded: bool,
    folder_filters: list[str],
    only_filter: str | None,
    limit: int | None,
) -> list[dict]:
    if not EXTRACTED_DIR.exists():
        raise FileNotFoundError(f"Extracted protocol directory not found: {EXTRACTED_DIR}")

    normalized_folders = [normalize_ascii(item) for item in folder_filters]
    normalized_only = normalize_ascii(only_filter) if only_filter else None

    entries: list[dict] = []
    for text_file in sorted(EXTRACTED_DIR.rglob("*.txt")):
        if looks_like_auxiliary_text(text_file):
            continue

        relative_path = text_file.relative_to(EXTRACTED_DIR)
        relative_key = normalize_relpath(relative_path)
        seeded_default = relative_key in SEEDED_TOP10_FILTER_TAGS
        if seeded_default and not include_seeded:
            continue

        folder_name = normalize_ascii(text_file.parent.name)
        if normalized_folders and folder_name not in normalized_folders:
            continue

        if normalized_only:
            filter_tags = sorted(SEEDED_TOP10_FILTER_TAGS.get(relative_key, set()))
            searchable = " | ".join(
                [
                    relative_key,
                    normalize_ascii(text_file.stem),
                    folder_name,
                    " ".join(filter_tags),
                ]
            )
            if normalized_only not in searchable:
                continue

        entries.append(
            {
                "text_file": text_file,
                "relative_path": relative_path,
                "relative_key": relative_key,
                "seeded_default": seeded_default,
                "domain_hint": default_domain_for_path(text_file),
            }
        )

    if limit is not None:
        entries = entries[:limit]

    return entries


def load_text(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Text file not found: {path}")
    return path.read_text(encoding="utf-8")


def infer_source_document_kind(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".txt":
        return "protocol_text"
    if suffix in {".md", ".markdown"}:
        return "protocol_markdown"
    if suffix == ".pdf":
        return "protocol_pdf"
    return "protocol_document"


def summarize_text_surface(text: str) -> dict:
    lines = text.splitlines()
    paragraphs = [item for item in text.split("\n\n") if item.strip()]
    words = [item for item in text.split() if item.strip()]
    return {
        "char_count": len(text),
        "line_count": len(lines),
        "paragraph_count": len(paragraphs),
        "word_count": len(words),
        "sha1": hashlib.sha1(text.encode("utf-8")).hexdigest(),
    }


def graph_contract_for_engine(engine: str, namespace: str) -> dict:
    if engine == "ontology_v2":
        return {
            "namespace": namespace,
            "logical_layers": [
                "L1_raw_evidence",
                "L2_canonical_models",
                "L3_assertions_and_rules",
            ],
            "core_nodes": [
                "RawDocument",
                "RawChunk",
                "DiseaseEntity",
                "ProtocolBook",
                "ProtocolSection",
                "ProtocolAssertion",
                "ProtocolDiseaseSummary",
                "RawSignMention",
                "RawServiceMention",
                "RawObservationMention",
            ],
            "reasoning_edges": [
                "CHUNK_ABOUT_DISEASE",
                "BOOK_HAS_SECTION",
                "SECTION_HAS_CHUNK",
                "SECTION_COVERS_DISEASE",
                "ASSERTION_REQUIRES_SIGN",
                "ASSERTION_INDICATES_SERVICE",
                "ASSERTION_CONTRAINDICATES",
            ],
        }
    return {
        "namespace": "legacy_universal",
        "logical_layers": [
            "L1_raw_evidence",
            "L2_canonical_models",
        ],
        "core_nodes": [
            "Disease",
            "Protocol",
            "Chunk",
            "Entity",
        ],
        "reasoning_edges": [],
    }


def save_snapshot(profile: DocumentProfile, payload: dict, *, suffix: str) -> Path:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    slug = _slugify(profile.disease_name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    snapshot_path = CONFIG_DIR / f"{slug}_{suffix}_{timestamp}.json"
    snapshot_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return snapshot_path


def summarize_result(engine: str, namespace: str, raw_result: dict) -> dict:
    if engine == "ontology_v2":
        mentions = (
            raw_result.get("signs", 0)
            + raw_result.get("services", 0)
            + raw_result.get("observations", 0)
        )
        return {
            "status": "success",
            "engine": engine,
            "namespace": namespace,
            "chunks": raw_result.get("chunks", 0),
            "mentions": mentions,
            "signs": raw_result.get("signs", 0),
            "services": raw_result.get("services", 0),
            "observations": raw_result.get("observations", 0),
            "assertions": raw_result.get("assertions", 0),
            "mappings": raw_result.get("mappings", {}),
            "stats": raw_result.get("stats", {}),
            "experience_advisory": raw_result.get("experience_advisory", {}),
        }

    result = dict(raw_result)
    result.setdefault("status", "success")
    result["engine"] = engine
    result["namespace"] = namespace
    return result


def write_manifest(manifest: dict) -> dict[str, str]:
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    disease_slug = _slugify(manifest["profile"]["disease_name"])
    stem = f"{disease_slug}_{timestamp}"
    json_path = MANIFEST_DIR / f"{stem}.json"
    md_path = MANIFEST_DIR / f"{stem}.md"
    json_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    source = manifest.get("source", {})
    result = manifest.get("result", {})
    lines = [
        f"# Text to Neo4j Manifest: {manifest['profile']['disease_name']}",
        "",
        f"- Engine: `{manifest.get('engine')}`",
        f"- Namespace: `{manifest.get('namespace')}`",
        f"- Source: `{source.get('relative_path')}`",
        f"- Source kind: `{source.get('document_kind')}`",
        f"- Source type: `{manifest['profile'].get('source_type')}`",
        f"- Status: `{result.get('status')}`",
        f"- Chunks: `{result.get('chunks', 0)}`",
        f"- Mentions: `{result.get('mentions', 0)}`",
        f"- Assertions: `{result.get('assertions', 0)}`",
        "",
        "## Graph Contract",
        "",
        f"- Logical layers: `{', '.join(manifest['graph_contract'].get('logical_layers', []))}`",
        f"- Core nodes: `{', '.join(manifest['graph_contract'].get('core_nodes', []))}`",
        f"- Reasoning edges: `{', '.join(manifest['graph_contract'].get('reasoning_edges', []))}`",
        "",
        "## Text Surface",
        "",
        f"- Chars: `{manifest['text_surface'].get('char_count', 0)}`",
        f"- Lines: `{manifest['text_surface'].get('line_count', 0)}`",
        f"- Paragraphs: `{manifest['text_surface'].get('paragraph_count', 0)}`",
        f"- SHA1: `{manifest['text_surface'].get('sha1', '')}`",
        "",
    ]
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return {"json": str(json_path), "md": str(md_path)}


def classify_profile_from_text(
    text_file: Path,
    text: str,
    analyzer: DocumentAnalyzer,
    source_type: str | None,
    hospital_name: str | None,
) -> DocumentProfile:
    sample = text[:5000]
    estimated_pages = max(1, len(text) // 2000)
    structure = analyzer.detect_structure(sample)
    data = analyzer.classify_text(
        sample_text=sample,
        total_pages=estimated_pages,
        quality="digital",
        structure=structure,
    )

    disease_name = text_file.stem
    disease_aliases: list[str] = []
    icd_code = ""
    medical_domain = default_domain_for_path(text_file)
    document_type = "treatment_guideline"
    publisher = "Bo Y te"
    year = None
    summary = sample[:500]
    resolved_source_type = source_type or "BYT"
    resolved_hospital_name = hospital_name

    if data:
        disease_name = data.get("disease_name") or disease_name
        disease_aliases = data.get("disease_aliases") or []
        icd_code = data.get("icd_code") or ""
        medical_domain = data.get("medical_domain") or medical_domain
        document_type = data.get("document_type") or document_type
        publisher = data.get("publisher") or publisher
        year = data.get("year")
        summary = data.get("summary") or summary

        publisher_key = normalize_ascii(publisher)
        if source_type is None:
            if publisher_key and all(token not in publisher_key for token in ("bo y te", "ministry of health", "byt")):
                if any(
                    token in publisher_key
                    for token in ("benh vien", "hospital", "vinmec", "bach mai", "cho ray", "trung tam", "vien")
                ):
                    resolved_source_type = "hospital"
                    resolved_hospital_name = publisher

    return DocumentProfile(
        disease_name=disease_name,
        disease_aliases=disease_aliases,
        icd_code=icd_code,
        medical_domain=medical_domain,
        document_type=document_type,
        pdf_quality="digital",
        has_tables=structure["has_tables"],
        has_flowcharts=structure["has_flowcharts"],
        has_appendices=structure["has_appendices"],
        heading_style=structure["heading_style"],
        estimated_pages=estimated_pages,
        publisher=publisher,
        year=year,
        summary=summary,
        source_type=resolved_source_type,
        hospital_name=resolved_hospital_name,
    )


def save_config_snapshot(profile: DocumentProfile, config_json: str, *, suffix: str) -> Path:
    payload = json.loads(config_json)
    return save_snapshot(profile, payload, suffix=suffix)


def load_existing_disease_keys(uri: str, user: str, password: str) -> set[str]:
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            records = session.run(
                """
                MATCH (d:Disease)
                WHERE d.name IS NOT NULL
                RETURN d.name AS name
                UNION
                MATCH (d:CIDisease)
                WHERE d.name IS NOT NULL
                RETURN d.name AS name
                UNION
                MATCH (d:DiseaseEntity)
                WHERE d.name IS NOT NULL
                RETURN d.name AS name
                """
            ).data()
    finally:
        driver.close()

    return {normalize_ascii(record["name"]) for record in records if record.get("name")}


def ingest_one(
    entry: dict,
    client: AzureOpenAI,
    model: str,
    analyzer: DocumentAnalyzer,
    existing_disease_keys: set[str] | None,
    engine: str,
    namespace: str,
    source_type: str | None,
    hospital_name: str | None,
) -> dict:
    text_file = entry["text_file"]
    relative_path = entry["relative_path"]
    t0 = time.time()

    print(f"\n{'#' * 72}")
    print(f"  Text file: {relative_path.as_posix()}")
    print(f"{'#' * 72}")

    text = load_text(text_file)
    text_surface = summarize_text_surface(text)
    print(f"  Text loaded: {text_surface['char_count']} chars | sha1={text_surface['sha1'][:12]}")

    profile = classify_profile_from_text(
        text_file=text_file,
        text=text,
        analyzer=analyzer,
        source_type=source_type,
        hospital_name=hospital_name,
    )
    print(
        "  Profile:"
        f" {profile.disease_name} | ICD={profile.icd_code or 'N/A'}"
        f" | domain={profile.medical_domain}"
        f" | source={profile.source_type}"
    )

    graph_contract = graph_contract_for_engine(engine, namespace)
    disease_key = normalize_ascii(profile.disease_name)
    if existing_disease_keys is not None and disease_key in existing_disease_keys:
        print("  Skip: disease already exists in Neo4j")
        result_summary = {
            "relative_path": relative_path.as_posix(),
            "disease": profile.disease_name,
            "icd": profile.icd_code,
            "engine": engine,
            "namespace": namespace,
            "status": "skipped_existing",
            "text_sha1": text_surface["sha1"],
            "elapsed_s": round(time.time() - t0, 1),
        }
        manifest = {
            "generated_at": datetime.now().astimezone().isoformat(),
            "engine": engine,
            "namespace": namespace,
            "source": {
                "relative_path": relative_path.as_posix(),
                "absolute_path": str(text_file),
                "document_kind": infer_source_document_kind(text_file),
                "seeded_default": entry["seeded_default"],
                "domain_hint": entry["domain_hint"],
            },
            "profile": profile.model_dump(mode="json"),
            "text_surface": text_surface,
            "graph_contract": graph_contract,
            "result": result_summary,
        }
        manifest_paths = write_manifest(manifest)
        result_summary["manifest_json"] = manifest_paths["json"]
        result_summary["manifest_md"] = manifest_paths["md"]
        return result_summary

    snapshot_payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "engine": engine,
        "namespace": namespace,
        "source": {
            "relative_path": relative_path.as_posix(),
            "absolute_path": str(text_file),
            "document_kind": infer_source_document_kind(text_file),
            "seeded_default": entry["seeded_default"],
            "domain_hint": entry["domain_hint"],
        },
        "profile": profile.model_dump(mode="json"),
        "text_surface": text_surface,
        "graph_contract": graph_contract,
    }

    print("\n  [Phase 2] Preparing structured ingest contract...")
    if engine == "ontology_v2":
        config_path = save_snapshot(profile, snapshot_payload, suffix="ontology_v2_text_contract")
        print(f"  Contract saved: {config_path.name}")
        ingestor = OntologyV2Ingest(namespace=namespace)
        try:
            raw_result = ingestor.run(
                pdf_path=str(text_file),
                disease_name=profile.disease_name,
                pre_extracted_text=text,
                skip_first_page=False,
                source_type=profile.source_type,
            )
        finally:
            ingestor.close()
    else:
        configurator = PipelineConfigurator(client, model)
        config = configurator.configure(profile)
        config.protocol_name = f"{profile.disease_name} (text protocol)"
        config.skip_first_page = False

        snapshot_payload["universal_pipeline_config"] = config.model_dump(mode="json")
        config_path = save_config_snapshot(
            profile,
            config.model_dump_json(indent=2, ensure_ascii=False),
            suffix="universal_text_contract",
        )
        print(f"  Contract saved: {config_path.name}")

        ingestor = UniversalIngest()
        try:
            raw_result = ingestor.run(
                pdf_path=str(text_file),
                config=config,
                pre_extracted_text=text,
            )
        finally:
            ingestor.close()

    result = summarize_result(engine, namespace, raw_result)

    elapsed = time.time() - t0
    print(
        f"\n  Done in {elapsed:.0f}s"
        f" | chunks={result.get('chunks', 0)}"
        f" | mentions={result.get('mentions', 0)}"
        f" | assertions={result.get('assertions', 0)}"
    )

    if existing_disease_keys is not None:
        existing_disease_keys.add(disease_key)

    result_summary = {
        "engine": engine,
        "namespace": namespace,
        "relative_path": relative_path.as_posix(),
        "disease": profile.disease_name,
        "icd": profile.icd_code,
        "text_sha1": text_surface["sha1"],
        "chunks": result.get("chunks", 0),
        "mentions": result.get("mentions", 0),
        "assertions": result.get("assertions", 0),
        "elapsed_s": round(elapsed, 1),
        "status": result.get("status", "unknown"),
        "config_snapshot": str(config_path),
    }
    manifest = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "engine": engine,
        "namespace": namespace,
        "source": {
            "relative_path": relative_path.as_posix(),
            "absolute_path": str(text_file),
            "document_kind": infer_source_document_kind(text_file),
            "seeded_default": entry["seeded_default"],
            "domain_hint": entry["domain_hint"],
        },
        "profile": profile.model_dump(mode="json"),
        "text_surface": text_surface,
        "graph_contract": graph_contract,
        "artifacts": {
            "config_snapshot": str(config_path),
        },
        "result": {
            **result,
            **result_summary,
        },
    }
    manifest_paths = write_manifest(manifest)
    result_summary["manifest_json"] = manifest_paths["json"]
    result_summary["manifest_md"] = manifest_paths["md"]
    return result_summary


def write_report(results: list[dict], *, engine: str, namespace: str, include_seeded: bool) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "engine": engine,
        "namespace": namespace,
        "include_seeded": include_seeded,
        "result_count": len(results),
        "items": results,
    }
    report_path = REPORT_DIR / f"batch_text_to_neo4j_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    report_path.write_text(json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return report_path


def build_arg_parser():
    import argparse

    parser = argparse.ArgumentParser(
        description="Batch text-first ingestion for protocol text files outside the original seeded 10."
    )
    parser.add_argument("--only", help="Substring filter on file name, relative path, or disease name.")
    parser.add_argument(
        "--folder",
        action="append",
        default=[],
        help="Folder name under Extracted_Protocols (repeatable). Example: --folder \"Phac do tai mui hong\"",
    )
    parser.add_argument("--limit", type=int, help="Limit number of text files after filtering.")
    parser.add_argument("--dry-run", action="store_true", help="List matching files without ingesting.")
    parser.add_argument("--skip-existing", action="store_true", help="Skip diseases already present in Neo4j.")
    parser.add_argument("--include-seeded", action="store_true", help="Include the original seeded top 10 files.")
    parser.add_argument(
        "--engine",
        choices=["ontology_v2", "universal"],
        default=DEFAULT_ENGINE,
        help="Ingestion engine. ontology_v2 is the richer, reasoning-first path.",
    )
    parser.add_argument(
        "--namespace",
        default=DEFAULT_NAMESPACE,
        help="Target Neo4j namespace. Default keeps text knowledge under ontology_v2.",
    )
    parser.add_argument("--source-type", choices=["BYT", "hospital"], help="Override source type for all inputs.")
    parser.add_argument("--hospital-name", help="Hospital name when --source-type hospital is used.")
    return parser


def main():
    ensure_pathway_data_layout()
    parser = build_arg_parser()
    args = parser.parse_args()

    entries = discover_text_files(
        include_seeded=args.include_seeded,
        folder_filters=args.folder,
        only_filter=args.only,
        limit=args.limit,
    )

    if not entries:
        print("No matching text files found.")
        return

    if args.dry_run:
        print(f"\n{'=' * 80}")
        print(f"  TEXT-FIRST CATALOG ({len(entries)} files)")
        print(f"{'=' * 80}")
        for index, entry in enumerate(entries, 1):
            seeded_tag = "seeded" if entry["seeded_default"] else "new"
            size_bytes = entry["text_file"].stat().st_size
            print(
                f"  [{index:03d}] {entry['relative_path'].as_posix()} "
                f"| {seeded_tag} | {size_bytes:,} bytes | engine={args.engine} | ns={args.namespace}"
            )
        return

    client = AzureOpenAI(
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    )
    model = os.getenv("MODEL1", "gpt-4o-mini")
    analyzer = DocumentAnalyzer(client, model)

    existing_disease_keys = None
    if args.skip_existing:
        neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7688")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password123")
        existing_disease_keys = load_existing_disease_keys(neo4j_uri, neo4j_user, neo4j_password)
        print(f"Loaded {len(existing_disease_keys)} existing disease keys from Neo4j")

    print(f"\n{'=' * 80}")
    print(f"  BATCH TEXT -> NEO4J")
    print(
        f"  files={len(entries)} | model={model} | engine={args.engine}"
        f" | namespace={args.namespace} | include_seeded={args.include_seeded}"
    )
    print(f"{'=' * 80}")

    results: list[dict] = []
    total_t0 = time.time()

    for index, entry in enumerate(entries, 1):
        print(f"\n>>> [{index}/{len(entries)}] {entry['relative_path'].as_posix()}")
        try:
            result = ingest_one(
                entry=entry,
                client=client,
                model=model,
                analyzer=analyzer,
                existing_disease_keys=existing_disease_keys,
                engine=args.engine,
                namespace=args.namespace,
                source_type=args.source_type,
                hospital_name=args.hospital_name,
            )
        except Exception as exc:
            traceback.print_exc()
            result = {
                "engine": args.engine,
                "namespace": args.namespace,
                "relative_path": entry["relative_path"].as_posix(),
                "status": "error",
                "error": str(exc),
            }
        results.append(result)

    total_elapsed = time.time() - total_t0
    report_path = write_report(
        results,
        engine=args.engine,
        namespace=args.namespace,
        include_seeded=args.include_seeded,
    )

    ok = [item for item in results if item.get("status") == "success"]
    skipped = [item for item in results if item.get("status") == "skipped_existing"]
    failed = [item for item in results if item.get("status") not in {"success", "skipped_existing"}]
    total_chunks = sum(item.get("chunks", 0) for item in ok)
    total_mentions = sum(item.get("mentions", 0) for item in ok)

    print(f"\n{'=' * 80}")
    print("  SUMMARY")
    print(f"{'=' * 80}")
    print(
        f"  success={len(ok)} | skipped_existing={len(skipped)} | failed={len(failed)}"
        f" | chunks={total_chunks} | mentions={total_mentions}"
    )
    print(f"  elapsed={total_elapsed:.0f}s ({total_elapsed / 60:.1f} min)")
    print(f"  report={report_path}")


if __name__ == "__main__":
    main()
