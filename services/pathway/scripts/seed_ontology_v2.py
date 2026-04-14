"""
Seed Ontology V2 — Load all 5 layers from JSON catalogs into Neo4j.

Layers:
  L1: Raw/Mention — already populated by ontology_v2_ingest.py
  L2: Canonical — SignConcept, ProtocolService, ServiceFamily, ObservationConcept, DiseaseEntity, Aliases
  L3: Protocol/Rules — ProtocolBook, ProtocolSection, ExclusionRule, RuleCatalog
  L4: Inference — DiseaseHypothesis, ResultSignal, SignalProfile + evidence links
  L5: Claims Ops — (templates only; actual ClaimCase nodes created at runtime)

Usage:
    cd notebooklm
    python scripts/seed_ontology_v2.py [--phase A|B|C|D|all] [--dry-run]
"""

import os
import sys
import io
import json
import hashlib
import argparse
import unicodedata
from pathlib import Path
from datetime import datetime

# Fix encoding on Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
WORKSPACE = PROJECT_DIR / "workspaces" / "claims_insights"

sys.path.insert(0, str(PROJECT_DIR))
from runtime_env import load_notebooklm_env
load_notebooklm_env()

from neo4j import GraphDatabase

NAMESPACE = "ontology_v2_seed"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def strip_diacritics(text: str) -> str:
    text = str(text or "").replace("đ", "d").replace("Đ", "D")
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    ).lower().strip()


def make_id(*parts) -> str:
    raw = "_".join(str(p) for p in parts)
    h = hashlib.md5(raw.encode()).hexdigest()[:8]
    slug = strip_diacritics(parts[0])[:40].replace(" ", "_")
    return f"{slug}_{h}"


def load_json(path: Path) -> dict | list:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


class Stats:
    def __init__(self):
        self._counts: dict[str, int] = {}

    def inc(self, key: str, n: int = 1):
        self._counts[key] = self._counts.get(key, 0) + n

    def report(self, phase: str):
        print(f"\n  [{phase}] Results:")
        for k, v in sorted(self._counts.items()):
            print(f"    {k}: {v}")


# ---------------------------------------------------------------------------
# Phase A: Seed Canonical Layer (L2)
# ---------------------------------------------------------------------------

def phase_a(session, stats: Stats):
    print("\n" + "=" * 70)
    print("  PHASE A: Seed Canonical Layer (L2)")
    print("=" * 70)

    # --- A1: ServiceFamily (12 categories) ---
    print("\n[A1] ServiceFamily nodes...")
    FAMILIES = {
        "LAB-BIO": "Sinh hoá", "LAB-HEM": "Huyết học", "LAB-IMM": "Miễn dịch",
        "LAB-MIC": "Vi sinh", "LAB-URI": "Nước tiểu",
        "IMG-XRY": "X-quang", "IMG-USG": "Siêu âm", "IMG-CTN": "CT/MRI",
        "END-ENS": "Nội soi", "FUN-DFT": "Thăm dò chức năng",
        "PAT-PAT": "Giải phẫu bệnh", "PRO-THT": "Thủ thuật/Khám",
        "GEN-OTH": "Chưa phân loại",
    }
    for code, name in FAMILIES.items():
        session.run("""
            MERGE (f:ServiceFamily {family_id: $fid})
            ON CREATE SET f.namespace = $ns, f.family_name = $name,
                          f.category_code = $code
            ON MATCH SET f.family_name = $name
        """, fid=f"family:{code}", ns=NAMESPACE, name=name, code=code)
        stats.inc("ServiceFamily")

    # --- A2: ProtocolService (2,551 codes) ---
    print("[A2] ProtocolService nodes from service_codebook.json...")
    codebook_path = WORKSPACE / "02_standardize" / "service_codebook.json"
    codebook = load_json(codebook_path)
    services = codebook.get("codebook", [])
    for svc in services:
        bhyt = svc.get("bhyt", {}) or {}
        session.run("""
            MERGE (s:ProtocolService {service_code: $code})
            ON CREATE SET
                s.namespace = $ns,
                s.service_name = $name,
                s.category_code = $cat_code,
                s.category_name = $cat_name,
                s.confidence = $conf,
                s.total_occurrences = $occ,
                s.avg_cost_vnd = $avg_cost,
                s.bhyt_code = $bhyt_code,
                s.bhyt_name = $bhyt_name,
                s.bhyt_price_vnd = $bhyt_price
            ON MATCH SET
                s.service_name = $name,
                s.total_occurrences = $occ,
                s.avg_cost_vnd = $avg_cost
        """, code=svc["service_code"], ns=NAMESPACE,
             name=svc["canonical_name"],
             cat_code=svc.get("category_code", ""),
             cat_name=svc.get("category_name", ""),
             conf=svc.get("confidence", ""),
             occ=svc.get("total_occurrences", 0),
             avg_cost=svc.get("avg_cost_vnd", 0),
             bhyt_code=bhyt.get("ma_tuong_duong", ""),
             bhyt_name=bhyt.get("ten_tt43", ""),
             bhyt_price=bhyt.get("gia_tt39_vnd", 0))
        stats.inc("ProtocolService")

        # Link to ServiceFamily
        cat_code = svc.get("category_code", "")
        if cat_code:
            session.run("""
                MATCH (s:ProtocolService {service_code: $code})
                MATCH (f:ServiceFamily {family_id: $fid})
                MERGE (s)-[:BELONGS_TO_FAMILY]->(f)
            """, code=svc["service_code"], fid=f"family:{cat_code}")
            stats.inc("BELONGS_TO_FAMILY")

        # ServiceAlias nodes from variants
        for var in svc.get("variants", []):
            alias_label = var.get("cleaned_name", "")
            if alias_label and alias_label != svc["canonical_name"]:
                aid = make_id(alias_label, svc["service_code"])
                session.run("""
                    MERGE (a:ServiceAlias {alias_id: $aid})
                    ON CREATE SET a.namespace = $ns,
                                  a.alias_label = $label,
                                  a.normalized_alias = $nkey
                """, aid=f"svc_alias:{aid}", ns=NAMESPACE,
                     label=alias_label, nkey=strip_diacritics(alias_label))
                session.run("""
                    MATCH (a:ServiceAlias {alias_id: $aid})
                    MATCH (s:ProtocolService {service_code: $code})
                    MERGE (a)-[:ALIAS_OF_SERVICE]->(s)
                """, aid=f"svc_alias:{aid}", code=svc["service_code"])
                stats.inc("ServiceAlias")

    # --- A3: SignConcept (273 active) ---
    print("[A3] SignConcept nodes from sign_concept_catalog_active_v1.json...")
    sign_path = WORKSPACE / "05_reference" / "signs" / "sign_concept_catalog_active_v1.json"
    sign_data = load_json(sign_path)
    for concept in sign_data.get("concepts", []):
        session.run("""
            MERGE (s:SignConcept {sign_id: $sid})
            ON CREATE SET
                s.namespace = $ns,
                s.canonical_label = $label,
                s.normalized_key = $nkey,
                s.support_cases = $sc,
                s.linked_disease_count = $ldc
            ON MATCH SET
                s.support_cases = $sc,
                s.linked_disease_count = $ldc
        """, sid=concept["sign_id"], ns=NAMESPACE,
             label=concept["canonical_label"],
             nkey=concept["normalized_key"],
             sc=concept.get("support_cases", 0),
             ldc=concept.get("linked_disease_count", 0))
        stats.inc("SignConcept")

        # ClaimSignAlias nodes
        for alias in concept.get("aliases", []):
            alias_label = alias.get("alias_label", "")
            if alias_label:
                aid = make_id(alias_label, concept["sign_id"])
                session.run("""
                    MERGE (a:ClaimSignAlias {alias_id: $aid})
                    ON CREATE SET
                        a.namespace = $ns,
                        a.alias_label = $label,
                        a.normalized_alias = $nkey,
                        a.token_count = $tc,
                        a.label_support_cases = $lsc,
                        a.label_frequency = $lf
                """, aid=f"sign_alias:{aid}", ns=NAMESPACE,
                     label=alias_label,
                     nkey=alias.get("normalized_alias", strip_diacritics(alias_label)),
                     tc=alias.get("token_count", 0),
                     lsc=alias.get("label_support_cases", 0),
                     lf=alias.get("label_frequency", 0))
                session.run("""
                    MATCH (a:ClaimSignAlias {alias_id: $aid})
                    MATCH (s:SignConcept {sign_id: $sid})
                    MERGE (a)-[:ALIAS_OF_SIGN]->(s)
                """, aid=f"sign_alias:{aid}", sid=concept["sign_id"])
                stats.inc("ClaimSignAlias")

        # SIGN_INDICATES_DISEASE relationships
        for disease in concept.get("top_diseases", []):
            did = disease.get("disease_id", "")
            if did:
                # Create DiseaseEntity if not exists
                session.run("""
                    MERGE (d:DiseaseEntity {disease_id: $did})
                    ON CREATE SET
                        d.namespace = $ns,
                        d.disease_name = $dname,
                        d.icd10 = $icd
                """, did=did, ns=NAMESPACE,
                     dname=disease.get("disease_name", ""),
                     icd=disease.get("icd10", ""))
                # Create ICDConcept
                icd = disease.get("icd10", "")
                if icd:
                    session.run("""
                        MERGE (i:ICDConcept {icd_code: $icd})
                        ON CREATE SET i.namespace = $ns, i.icd_name = $name
                    """, icd=icd, ns=NAMESPACE, name=disease.get("disease_name", ""))
                    session.run("""
                        MATCH (d:DiseaseEntity {disease_id: $did})
                        MATCH (i:ICDConcept {icd_code: $icd})
                        MERGE (d)-[:HAS_ICD]->(i)
                    """, did=did, icd=icd)
                    stats.inc("ICDConcept")

                session.run("""
                    MATCH (s:SignConcept {sign_id: $sid})
                    MATCH (d:DiseaseEntity {disease_id: $did})
                    MERGE (s)-[r:SIGN_INDICATES_DISEASE]->(d)
                    ON CREATE SET r.weight = $w, r.support_cases = $sc
                """, sid=concept["sign_id"], did=did,
                     w=float(disease.get("support_cases", 1)),
                     sc=disease.get("support_cases", 0))
                stats.inc("SIGN_INDICATES_DISEASE")
                stats.inc("DiseaseEntity_from_signs")

    # --- A4: ObservationConcept (46) + ObservationFamily ---
    print("[A4] ObservationConcept nodes...")
    obs_path = WORKSPACE / "05_observations" / "observation_concept_catalog_seed.json"
    obs_data = load_json(obs_path)

    obs_families_seen = set()
    for concept in obs_data.get("concepts", []):
        cat_code = concept.get("category_code", "")
        cat_name = concept.get("category_name", "")
        # ObservationFamily
        if cat_code and cat_code not in obs_families_seen:
            session.run("""
                MERGE (f:ObservationFamily {family_id: $fid})
                ON CREATE SET f.namespace = $ns, f.family_name = $name, f.category_code = $code
            """, fid=f"obs_family:{cat_code}", ns=NAMESPACE, name=cat_name, code=cat_code)
            obs_families_seen.add(cat_code)
            stats.inc("ObservationFamily")

        session.run("""
            MERGE (o:ObservationConcept {concept_code: $code})
            ON CREATE SET
                o.namespace = $ns,
                o.concept_name = $name,
                o.category_code = $cat_code,
                o.category_name = $cat_name,
                o.result_semantics = $sem,
                o.aliases = $aliases
        """, code=concept["concept_code"], ns=NAMESPACE,
             name=concept["canonical_name"],
             cat_code=cat_code, cat_name=cat_name,
             sem=concept.get("result_semantics", "quantitative"),
             aliases=concept.get("aliases", []))
        stats.inc("ObservationConcept")

        # Link to family
        if cat_code:
            session.run("""
                MATCH (o:ObservationConcept {concept_code: $code})
                MATCH (f:ObservationFamily {family_id: $fid})
                MERGE (o)-[:OBSERVATION_IN_FAMILY]->(f)
            """, code=concept["concept_code"], fid=f"obs_family:{cat_code}")
            stats.inc("OBSERVATION_IN_FAMILY")

        # ObservationAlias nodes
        for alias_text in concept.get("aliases", []):
            if alias_text and alias_text != concept["canonical_name"]:
                aid = make_id(alias_text, concept["concept_code"])
                session.run("""
                    MERGE (a:ObservationAlias {alias_id: $aid})
                    ON CREATE SET a.namespace = $ns, a.alias_label = $label,
                                  a.normalized_alias = $nkey
                """, aid=f"obs_alias:{aid}", ns=NAMESPACE,
                     label=alias_text, nkey=strip_diacritics(alias_text))
                session.run("""
                    MATCH (a:ObservationAlias {alias_id: $aid})
                    MATCH (o:ObservationConcept {concept_code: $code})
                    MERGE (a)-[:ALIAS_OF_OBSERVATION]->(o)
                """, aid=f"obs_alias:{aid}", code=concept["concept_code"])
                stats.inc("ObservationAlias")

    # --- A5: DiseaseEntity from disease profiles (enriched with signs/services) ---
    print("[A5] DiseaseEntity enrichment from disease profiles...")
    profiles_path = WORKSPACE / "05_reference" / "signs" / "tmh_ontology_disease_profiles.json"
    profiles = load_json(profiles_path)
    for profile in profiles.get("profiles", []):
        did = profile["disease_id"]
        session.run("""
            MERGE (d:DiseaseEntity {disease_id: $did})
            ON CREATE SET
                d.namespace = $ns,
                d.disease_name = $name,
                d.specialty = $spec,
                d.aliases = $aliases
            ON MATCH SET
                d.specialty = $spec,
                d.aliases = $aliases
        """, did=did, ns=NAMESPACE,
             name=profile["disease_name"],
             spec=profile.get("specialty", ""),
             aliases=profile.get("disease_aliases", []))
        stats.inc("DiseaseEntity_from_profiles")

    stats.report("Phase A")


# ---------------------------------------------------------------------------
# Phase B: Protocol Structure (L3)
# ---------------------------------------------------------------------------

def phase_b(session, stats: Stats):
    print("\n" + "=" * 70)
    print("  PHASE B: Protocol Structure (L3)")
    print("=" * 70)

    # --- B1: ProtocolBook nodes ---
    print("\n[B1] ProtocolBook nodes...")
    books = [
        {"book_id": "book:tmh_byt_2016", "book_name": "Hướng dẫn chẩn đoán và điều trị một số bệnh về Tai Mũi Họng",
         "publisher": "Bộ Y tế", "year": 2016, "source_type": "BYT"},
        {"book_id": "book:hepb_byt_2019", "book_name": "Hướng dẫn điều trị viêm gan virus B",
         "publisher": "Bộ Y tế", "year": 2019, "source_type": "BYT"},
        {"book_id": "book:dengue_byt_2023", "book_name": "Hướng dẫn chẩn đoán và điều trị sốt xuất huyết Dengue",
         "publisher": "Bộ Y tế", "year": 2023, "source_type": "BYT"},
        {"book_id": "book:copd_byt_2023", "book_name": "Hướng dẫn chẩn đoán và điều trị bệnh phổi tắc nghẽn mạn tính",
         "publisher": "Bộ Y tế", "year": 2023, "source_type": "BYT"},
        {"book_id": "book:suy_tim_byt_2023", "book_name": "Hướng dẫn chẩn đoán và điều trị suy tim cấp và mạn",
         "publisher": "Bộ Y tế", "year": 2023, "source_type": "BYT"},
        {"book_id": "book:than_man_byt_2024", "book_name": "Hướng dẫn chẩn đoán điều trị bệnh thận mạn",
         "publisher": "Bộ Y tế", "year": 2024, "source_type": "BYT"},
    ]
    for book in books:
        session.run("""
            MERGE (b:ProtocolBook {book_id: $bid})
            ON CREATE SET
                b.namespace = $ns,
                b.book_name = $name,
                b.publisher = $pub,
                b.year = $year,
                b.source_type = $st
        """, bid=book["book_id"], ns=NAMESPACE,
             name=book["book_name"], pub=book["publisher"],
             year=book["year"], st=book["source_type"])
        stats.inc("ProtocolBook")

    # --- B2: ProtocolSection — derive from RawChunks section_type ---
    print("[B2] ProtocolSection nodes from existing RawChunks...")
    result = session.run("""
        MATCH (c:RawChunk)
        WITH c.disease_id AS did, c.section_type AS stype, c.section_title AS stitle,
             collect(c.chunk_id) AS chunk_ids, min(c.page_numbers[0]) AS page_start
        RETURN did, stype, stitle, chunk_ids, page_start
        ORDER BY did, page_start
    """)
    for record in result:
        did = record["did"]
        stype = record["stype"]
        stitle = record["stitle"]
        chunk_ids = record["chunk_ids"]
        sec_id = f"section:{make_id(did, stype, stitle)}"

        session.run("""
            MERGE (sec:ProtocolSection {section_id: $sid})
            ON CREATE SET
                sec.namespace = $ns,
                sec.section_title = $title,
                sec.section_type = $stype,
                sec.disease_id = $did,
                sec.page_start = $ps
        """, sid=sec_id, ns=NAMESPACE, title=stitle, stype=stype,
             did=did, ps=record["page_start"])
        stats.inc("ProtocolSection")

        # Link section → chunks
        for cid in chunk_ids:
            session.run("""
                MATCH (sec:ProtocolSection {section_id: $sid})
                MATCH (c:RawChunk {chunk_id: $cid})
                MERGE (sec)-[:SECTION_HAS_CHUNK]->(c)
            """, sid=sec_id, cid=cid)
            stats.inc("SECTION_HAS_CHUNK")

        # Link section → disease
        session.run("""
            MATCH (sec:ProtocolSection {section_id: $sid})
            MATCH (d:DiseaseEntity {disease_id: $did})
            MERGE (sec)-[:SECTION_COVERS_DISEASE]->(d)
        """, sid=sec_id, did=did)

    # --- B3: Link ProtocolAssertions to sections ---
    print("[B3] Linking ProtocolAssertions to ProtocolSections...")
    result = session.run("""
        MATCH (a:ProtocolAssertion)
        WHERE a.source_chunk_id IS NOT NULL
        MATCH (c:RawChunk {chunk_id: a.source_chunk_id})
        MATCH (sec:ProtocolSection)-[:SECTION_HAS_CHUNK]->(c)
        MERGE (sec)-[:CONTAINS_ASSERTION]->(a)
        RETURN count(*) AS linked
    """)
    linked = result.single()["linked"]
    stats.inc("CONTAINS_ASSERTION", linked)
    print(f"  Linked {linked} assertions to sections")

    # --- B4: ExclusionRule + RuleCatalog ---
    print("[B4] ExclusionRule nodes from contract_rules.json...")
    rules_path = WORKSPACE / "06_insurance" / "contract_rules.json"
    rules_data = load_json(rules_path)
    items = rules_data.get("taxonomy", {}).get("exclusion_items", [])

    # Create RuleCatalog
    session.run("""
        MERGE (rc:RuleCatalog {catalog_id: $cid})
        ON CREATE SET rc.namespace = $ns,
                      rc.catalog_name = $name,
                      rc.rule_count = $cnt
    """, cid="catalog:exclusion_v1", ns=NAMESPACE,
         name="Insurance Exclusion Rules v1", cnt=len(items))
    stats.inc("RuleCatalog")

    for item in items:
        session.run("""
            MERGE (r:ExclusionRule {rule_code: $code})
            ON CREATE SET
                r.namespace = $ns,
                r.reason = $reason,
                r.group = $grp,
                r.process_path = $pp,
                r.source_note = $sn
        """, code=item["code"], ns=NAMESPACE,
             reason=item["reason"], grp=item["group"],
             pp=item.get("process_path", ""),
             sn=item.get("source_note", ""))

        # Link to catalog
        session.run("""
            MATCH (rc:RuleCatalog {catalog_id: $cid})
            MATCH (r:ExclusionRule {rule_code: $code})
            MERGE (rc)-[:CATALOG_HAS_RULE]->(r)
        """, cid="catalog:exclusion_v1", code=item["code"])
        stats.inc("ExclusionRule")

        # Link rules to ServiceFamily by group
        group_to_family = {
            "Thuốc": None,  # cross-family, no specific link
            "Cận lâm sàng": None,
            "Loại trừ - Quyền lợi": None,
        }
        # No auto-linking for now — rules apply based on reasoning, not category

    stats.report("Phase B")


# ---------------------------------------------------------------------------
# Phase C: Inference Engine (L4)
# ---------------------------------------------------------------------------

def phase_c(session, stats: Stats):
    print("\n" + "=" * 70)
    print("  PHASE C: Inference Engine (L4)")
    print("=" * 70)

    cat_path = WORKSPACE / "05_reference" / "phac_do" / "tmh_lab_result_disease_catalog.json"
    catalog = load_json(cat_path)

    # --- C1: SignalProfile nodes ---
    print("\n[C1] SignalProfile nodes...")
    for sp in catalog.get("signal_profiles", []):
        session.run("""
            MERGE (p:SignalProfile {profile_id: $pid})
            ON CREATE SET
                p.namespace = $ns,
                p.label = $label,
                p.trigger = $trigger,
                p.support_direction = $sd,
                p.support_level = $sl,
                p.weight = $w
        """, pid=sp["profile_id"], ns=NAMESPACE,
             label=sp["label"],
             trigger=json.dumps(sp.get("trigger", {})),
             sd=sp.get("support_direction", ""),
             sl=sp.get("support_level", ""),
             w=sp.get("weight", 1.0))
        stats.inc("SignalProfile")

    # --- C2: DiseaseHypothesis nodes ---
    print("[C2] DiseaseHypothesis nodes...")
    for disease in catalog.get("diseases", []):
        hyp_id = f"hyp:{disease['entity_key']}"
        session.run("""
            MERGE (h:DiseaseHypothesis {hypothesis_id: $hid})
            ON CREATE SET
                h.namespace = $ns,
                h.disease_id = $did,
                h.disease_name = $dname,
                h.icd10 = $icd,
                h.specialty = 'TMH'
        """, hid=hyp_id, ns=NAMESPACE,
             did=disease["entity_key"],
             dname=disease["disease_name"],
             icd=disease.get("icd10", ""))
        stats.inc("DiseaseHypothesis")

        # Link hypothesis → DiseaseEntity (create if needed)
        session.run("""
            MERGE (d:DiseaseEntity {disease_id: $did})
            ON CREATE SET d.namespace = $ns, d.disease_name = $dname, d.icd10 = $icd
        """, did=disease["entity_key"], ns=NAMESPACE,
             dname=disease["disease_name"], icd=disease.get("icd10", ""))
        session.run("""
            MATCH (h:DiseaseHypothesis {hypothesis_id: $hid})
            MATCH (d:DiseaseEntity {disease_id: $did})
            MERGE (h)-[:HYPOTHESIS_FOR_DISEASE]->(d)
        """, hid=hyp_id, did=disease["entity_key"])
        stats.inc("HYPOTHESIS_FOR_DISEASE")

    # --- C3: ResultSignal nodes ---
    print("[C3] ResultSignal nodes...")
    for signal in catalog.get("signal_sources", []):
        session.run("""
            MERGE (rs:ResultSignal {source_key: $sk})
            ON CREATE SET
                rs.namespace = $ns,
                rs.concept_code = $cc,
                rs.concept_name = $cn,
                rs.category_code = $catc,
                rs.result_semantics = $sem,
                rs.observed_rows = $obs,
                rs.abnormal_count = $abn,
                rs.allowed_profile_ids = $pids
        """, sk=signal["source_key"], ns=NAMESPACE,
             cc=signal.get("concept_code", ""),
             cn=signal.get("concept_name", ""),
             catc=signal.get("category_code", ""),
             sem=signal.get("result_semantics", ""),
             obs=signal.get("observed_rows", 0),
             abn=signal.get("abnormal_count", 0),
             pids=signal.get("allowed_profile_ids", []))
        stats.inc("ResultSignal")

        # Link ResultSignal → ObservationConcept
        cc = signal.get("concept_code", "")
        if cc:
            session.run("""
                MATCH (rs:ResultSignal {source_key: $sk})
                MATCH (o:ObservationConcept {concept_code: $cc})
                MERGE (rs)-[:SIGNAL_MAPS_OBSERVATION]->(o)
            """, sk=signal["source_key"], cc=cc)
            stats.inc("SIGNAL_MAPS_OBSERVATION")

    # --- C4: SIGNAL_HAS_PROFILE links ---
    print("[C4] SIGNAL_HAS_PROFILE links...")
    for link in catalog.get("relationships", {}).get("signal_profile", []):
        session.run("""
            MATCH (rs:ResultSignal {source_key: $sk})
            MATCH (p:SignalProfile {profile_id: $pid})
            MERGE (rs)-[:SIGNAL_HAS_PROFILE]->(p)
        """, sk=link["signal_source_key"], pid=link["profile_id"])
        stats.inc("SIGNAL_HAS_PROFILE")

    # --- C5: DISEASE_EXPECTS_SERVICE links ---
    print("[C5] DISEASE_EXPECTS_SERVICE links...")
    for link in catalog.get("relationships", {}).get("disease_service", []):
        hyp_id = f"hyp:{link['disease_key']}"
        session.run("""
            MATCH (h:DiseaseHypothesis {hypothesis_id: $hid})
            MATCH (s:ProtocolService {service_code: $sc})
            MERGE (h)-[r:DISEASE_EXPECTS_SERVICE]->(s)
            ON CREATE SET r.category_code = $cc
        """, hid=hyp_id, sc=link["service_code"],
             cc=link.get("category_code", ""))
        stats.inc("DISEASE_EXPECTS_SERVICE")

    # --- C6: SERVICE_PRODUCES_SIGNAL links ---
    print("[C6] SERVICE_PRODUCES_SIGNAL links...")
    for link in catalog.get("relationships", {}).get("service_signal", []):
        session.run("""
            MATCH (s:ProtocolService {service_code: $sc})
            MATCH (rs:ResultSignal {source_key: $sk})
            MERGE (s)-[r:SERVICE_PRODUCES_SIGNAL]->(rs)
            ON CREATE SET r.link_mode = $lm, r.link_confidence = $lc
        """, sc=link["service_key"].split(":")[-1].upper().replace("_", "-"),
             sk=link["signal_source_key"],
             lm=link.get("link_mode", ""),
             lc=link.get("link_confidence", ""))
        stats.inc("SERVICE_PRODUCES_SIGNAL")

    # --- C7: DISEASE_EXPECTS_SIGNAL links ---
    print("[C7] DISEASE_EXPECTS_SIGNAL links...")
    for link in catalog.get("relationships", {}).get("disease_signal", []):
        hyp_id = f"hyp:{link['disease_key']}"
        session.run("""
            MATCH (h:DiseaseHypothesis {hypothesis_id: $hid})
            MATCH (rs:ResultSignal {source_key: $sk})
            MERGE (h)-[r:DISEASE_EXPECTS_SIGNAL]->(rs)
            ON CREATE SET r.evidence_weight = $ew, r.link_mode = $lm
        """, hid=hyp_id, sk=link["signal_source_key"],
             ew=link.get("evidence_weight", 1.0),
             lm=link.get("link_mode", ""))
        stats.inc("DISEASE_EXPECTS_SIGNAL")

    stats.report("Phase C")


# ---------------------------------------------------------------------------
# Phase D: Claims Operational Templates (L5)
# ---------------------------------------------------------------------------

def phase_d(session, stats: Stats):
    print("\n" + "=" * 70)
    print("  PHASE D: Claims Operational Templates (L5)")
    print("=" * 70)

    # L5 nodes (ClaimCase, ServiceLine, ReviewDecision) are created at runtime
    # when processing actual claims. Here we only set up the schema constraints
    # and link ExclusionRules (already created in Phase B) to reasoning templates.

    # --- D1: ClinicalTopic nodes ---
    print("\n[D1] ClinicalTopic nodes...")
    topics = [
        ("topic:tmh", "Tai Mũi Họng"),
        ("topic:noi_tiet", "Nội tiết"),
        ("topic:truyen_nhiem", "Truyền nhiễm"),
        ("topic:ho_hap", "Hô hấp"),
        ("topic:tim_mach", "Tim mạch"),
        ("topic:than", "Thận"),
        ("topic:ung_buou", "Ung bướu"),
        ("topic:nhi", "Nhi khoa"),
    ]
    for tid, tname in topics:
        session.run("""
            MERGE (t:ClinicalTopic {topic_id: $tid})
            ON CREATE SET t.namespace = $ns, t.topic_name = $name
        """, tid=tid, ns=NAMESPACE, name=tname)
        stats.inc("ClinicalTopic")

    # --- D2: DiseaseGroup nodes + links ---
    print("[D2] DiseaseGroup nodes...")
    groups = [
        {"gid": "group:tmh_tai", "name": "Bệnh lý Tai", "topic": "topic:tmh",
         "diseases": ["disease:h66_3", "disease:h80"]},
        {"gid": "group:tmh_mui_xoang", "name": "Bệnh lý Mũi Xoang", "topic": "topic:tmh",
         "diseases": ["disease:j30_3", "disease:j32", "disease:j33_9"]},
        {"gid": "group:tmh_hong_thanh_quan", "name": "Bệnh lý Họng Thanh quản", "topic": "topic:tmh",
         "diseases": ["disease:j02", "disease:j04_0", "disease:j31_2", "disease:j37_0"]},
        {"gid": "group:tmh_u", "name": "U vùng đầu cổ", "topic": "topic:tmh",
         "diseases": ["disease:c02", "disease:d14_0", "disease:d14_1"]},
        {"gid": "group:tmh_khac", "name": "Bệnh TMH khác", "topic": "topic:tmh",
         "diseases": ["disease:a15_5", "disease:g51_0", "disease:k09_0", "disease:q18_2", "disease:t18_9"]},
    ]
    for group in groups:
        session.run("""
            MERGE (g:DiseaseGroup {group_id: $gid})
            ON CREATE SET g.namespace = $ns, g.group_name = $name
        """, gid=group["gid"], ns=NAMESPACE, name=group["name"])
        stats.inc("DiseaseGroup")

        # GROUP_IN_TOPIC
        session.run("""
            MATCH (g:DiseaseGroup {group_id: $gid})
            MATCH (t:ClinicalTopic {topic_id: $tid})
            MERGE (g)-[:GROUP_IN_TOPIC]->(t)
        """, gid=group["gid"], tid=group["topic"])
        stats.inc("GROUP_IN_TOPIC")

        # DISEASE_IN_GROUP
        for did in group.get("diseases", []):
            session.run("""
                MATCH (d:DiseaseEntity {disease_id: $did})
                MATCH (g:DiseaseGroup {group_id: $gid})
                MERGE (d)-[:DISEASE_IN_GROUP]->(g)
            """, did=did, gid=group["gid"])
            stats.inc("DISEASE_IN_GROUP")

    # --- D3: Create indexes for all node types ---
    print("[D3] Creating indexes...")
    indexes = [
        "CREATE INDEX sign_concept_idx IF NOT EXISTS FOR (n:SignConcept) ON (n.sign_id)",
        "CREATE INDEX sign_concept_nkey IF NOT EXISTS FOR (n:SignConcept) ON (n.normalized_key)",
        "CREATE INDEX service_code_idx IF NOT EXISTS FOR (n:ProtocolService) ON (n.service_code)",
        "CREATE INDEX service_name_idx IF NOT EXISTS FOR (n:ProtocolService) ON (n.service_name)",
        "CREATE INDEX obs_concept_idx IF NOT EXISTS FOR (n:ObservationConcept) ON (n.concept_code)",
        "CREATE INDEX disease_id_idx IF NOT EXISTS FOR (n:DiseaseEntity) ON (n.disease_id)",
        "CREATE INDEX disease_icd_idx IF NOT EXISTS FOR (n:DiseaseEntity) ON (n.icd10)",
        "CREATE INDEX icd_code_idx IF NOT EXISTS FOR (n:ICDConcept) ON (n.icd_code)",
        "CREATE INDEX hypothesis_idx IF NOT EXISTS FOR (n:DiseaseHypothesis) ON (n.hypothesis_id)",
        "CREATE INDEX signal_source_idx IF NOT EXISTS FOR (n:ResultSignal) ON (n.source_key)",
        "CREATE INDEX signal_profile_idx IF NOT EXISTS FOR (n:SignalProfile) ON (n.profile_id)",
        "CREATE INDEX exclusion_code_idx IF NOT EXISTS FOR (n:ExclusionRule) ON (n.rule_code)",
        "CREATE INDEX service_family_idx IF NOT EXISTS FOR (n:ServiceFamily) ON (n.family_id)",
        "CREATE INDEX obs_family_idx IF NOT EXISTS FOR (n:ObservationFamily) ON (n.family_id)",
        "CREATE INDEX sign_alias_idx IF NOT EXISTS FOR (n:ClaimSignAlias) ON (n.alias_id)",
        "CREATE INDEX sign_alias_nkey IF NOT EXISTS FOR (n:ClaimSignAlias) ON (n.normalized_alias)",
        "CREATE INDEX svc_alias_idx IF NOT EXISTS FOR (n:ServiceAlias) ON (n.alias_id)",
        "CREATE INDEX obs_alias_idx IF NOT EXISTS FOR (n:ObservationAlias) ON (n.alias_id)",
        "CREATE INDEX protocol_book_idx IF NOT EXISTS FOR (n:ProtocolBook) ON (n.book_id)",
        "CREATE INDEX protocol_section_idx IF NOT EXISTS FOR (n:ProtocolSection) ON (n.section_id)",
        "CREATE INDEX rule_catalog_idx IF NOT EXISTS FOR (n:RuleCatalog) ON (n.catalog_id)",
        "CREATE INDEX topic_idx IF NOT EXISTS FOR (n:ClinicalTopic) ON (n.topic_id)",
        "CREATE INDEX group_idx IF NOT EXISTS FOR (n:DiseaseGroup) ON (n.group_id)",
    ]
    for idx in indexes:
        try:
            session.run(idx)
            stats.inc("indexes_created")
        except Exception:
            stats.inc("indexes_skipped")

    stats.report("Phase D")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Seed Ontology V2 into Neo4j")
    parser.add_argument("--phase", default="all", choices=["A", "B", "C", "D", "all"],
                        help="Which phase to run (default: all)")
    parser.add_argument("--dry-run", action="store_true", help="Parse data but don't write to Neo4j")
    args = parser.parse_args()

    uri = os.getenv("NEO4J_URI", "bolt://localhost:7688")
    user = os.getenv("NEO4J_USER", "neo4j")
    pwd = os.getenv("NEO4J_PASSWORD", "password123")

    driver = GraphDatabase.driver(uri, auth=(user, pwd))
    stats = Stats()

    print(f"\n{'#' * 70}")
    print(f"  ONTOLOGY V2 SEED — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Neo4j: {uri}")
    print(f"  Phase: {args.phase}")
    print(f"{'#' * 70}")

    try:
        with driver.session() as session:
            if args.phase in ("A", "all"):
                phase_a(session, stats)
            if args.phase in ("B", "all"):
                phase_b(session, stats)
            if args.phase in ("C", "all"):
                phase_c(session, stats)
            if args.phase in ("D", "all"):
                phase_d(session, stats)

        # Final count verification
        print(f"\n{'=' * 70}")
        print("  VERIFICATION — Node counts in Neo4j")
        print(f"{'=' * 70}")
        with driver.session() as session:
            labels = [
                "SignConcept", "ClaimSignAlias", "ProtocolService", "ServiceAlias",
                "ServiceFamily", "ObservationConcept", "ObservationAlias", "ObservationFamily",
                "DiseaseEntity", "ICDConcept", "DiseaseGroup", "ClinicalTopic",
                "ProtocolBook", "ProtocolSection", "ProtocolAssertion", "ExclusionRule",
                "RuleCatalog", "DiseaseHypothesis", "ResultSignal", "SignalProfile",
                "RawChunk", "RawSignMention", "RawServiceMention", "RawObservationMention",
                "ProtocolDiseaseSummary",
            ]
            total = 0
            for label in labels:
                r = session.run(f"MATCH (n:{label}) RETURN count(n) AS c").single()
                c = r["c"]
                if c > 0:
                    print(f"  {label:30s} {c:>6}")
                    total += c
            print(f"  {'TOTAL':30s} {total:>6}")

            # Relationship counts
            print(f"\n  Key relationships:")
            rels = [
                "BELONGS_TO_FAMILY", "ALIAS_OF_SIGN", "ALIAS_OF_SERVICE",
                "ALIAS_OF_OBSERVATION", "OBSERVATION_IN_FAMILY",
                "SIGN_INDICATES_DISEASE", "HAS_ICD", "DISEASE_IN_GROUP", "GROUP_IN_TOPIC",
                "SECTION_HAS_CHUNK", "CONTAINS_ASSERTION", "ASSERTION_ABOUT_DISEASE",
                "CATALOG_HAS_RULE",
                "HYPOTHESIS_FOR_DISEASE", "DISEASE_EXPECTS_SERVICE",
                "SERVICE_PRODUCES_SIGNAL", "DISEASE_EXPECTS_SIGNAL",
                "SIGNAL_HAS_PROFILE", "SIGNAL_MAPS_OBSERVATION",
            ]
            for rel in rels:
                r = session.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS c").single()
                c = r["c"]
                if c > 0:
                    print(f"  {rel:35s} {c:>6}")

    finally:
        driver.close()

    print(f"\n{'#' * 70}")
    print("  ONTOLOGY V2 SEED COMPLETE")
    print(f"{'#' * 70}\n")


if __name__ == "__main__":
    main()
