"""
Seed Disease → ExpectedService relationships with clinical roles.

Covers all diseases from testcase batch 03-11 + common respiratory/infectious.
Each DISEASE_EXPECTS_SERVICE has a `role` property:
  - screening: routine workup for this disease
  - diagnostic: helps confirm the diagnosis
  - rule_out: used to exclude differential diagnoses
  - confirmatory: definitive diagnostic test
  - treatment: therapeutic service/drug
  - monitoring: follow-up/severity assessment
  - severity: assess disease severity/extent

Usage:
    cd notebooklm
    python scripts/seed_disease_service_expectations.py
"""

import os
import sys
import io
import json
from pathlib import Path

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

sys.path.insert(0, str(Path(__file__).parent.parent))
from runtime_env import load_notebooklm_env
load_notebooklm_env()

from neo4j import GraphDatabase

NAMESPACE = "ontology_v2_seed"

uri = os.getenv("NEO4J_URI", "bolt://localhost:7688")
driver = GraphDatabase.driver(uri, auth=("neo4j", "password123"))

# ---------------------------------------------------------------------------
# Disease → ExpectedService catalog
# Each entry: (service_code_or_keyword, service_name, role, category_code)
# ---------------------------------------------------------------------------

DISEASE_SERVICE_MAP = {
    # ─── TMH: Viêm mũi họng cấp tính (J00/J06) ───
    "Viêm mũi họng cấp tính": {
        "icd10": "J06.9",
        "specialty": "TMH",
        "aliases": ["viêm mũi họng cấp", "viêm họng cấp", "cảm cúm thông thường", "viêm VA cấp"],
        "services": [
            ("END-ENS-001", "nội soi tai mũi họng", "diagnostic", "END-ENS"),
            ("LAB-HEM-011", "công thức máu", "screening", "LAB-HEM"),
            ("LAB-HEM-039", "tổng phân tích tế bào máu", "screening", "LAB-HEM"),
            ("LAB-MIC-101", "kháng sinh đồ", "confirmatory", "LAB-MIC"),
            ("GEN-OTH-120", "streptococcus pyogenes aso", "rule_out", "GEN-OTH"),
            ("LAB-MIC-093", "afb trực tiếp nhuộm ziehl-neelsen", "rule_out", "LAB-MIC"),
        ],
    },

    # ─── TMH: Viêm mũi xoang cấp tính (J01) ───
    "Viêm mũi xoang cấp tính": {
        "icd10": "J01.9",
        "specialty": "TMH",
        "aliases": ["viêm xoang cấp", "viêm mũi xoang"],
        "services": [
            ("END-ENS-001", "nội soi tai mũi họng", "diagnostic", "END-ENS"),
            ("IMG-CTN-071", "chụp cắt lớp vi tính", "severity", "IMG-CTN"),
            ("IMG-CTN-053", "chụp cắt lớp vi tính lồng ngực", "severity", "IMG-CTN"),
            ("LAB-MIC-101", "kháng sinh đồ", "confirmatory", "LAB-MIC"),
        ],
    },

    # ─── TMH: Bệnh Ménière (H81.0) ───
    "Bệnh Ménière": {
        "icd10": "H81.0",
        "specialty": "TMH",
        "aliases": ["Meniere", "Ménière"],
        "services": [
            ("FUN-DFT-006", "đo thính lực đơn âm", "diagnostic", "FUN-DFT"),
            ("FUN-DFT-018", "điện động nhãn đồ", "diagnostic", "FUN-DFT"),
            ("FUN-DFT-012", "điện thính giác thân não", "diagnostic", "FUN-DFT"),
            ("IMG-CTN-004", "chụp cộng hưởng từ mri", "rule_out", "IMG-CTN"),
        ],
    },

    # ─── TMH: Viêm tai giữa mạn tính có cholesteatoma (H66.3) ───
    "Viêm tai giữa mạn tính có cholesteatoma": {
        "icd10": "H66.3",
        "specialty": "TMH",
        "aliases": ["cholesteatoma", "viêm tai giữa mạn cholesteatoma"],
        "services": [
            ("END-ENS-041", "khám tai dưới kính hiển vi", "diagnostic", "END-ENS"),
            ("IMG-CTN-071", "chụp cắt lớp vi tính", "severity", "IMG-CTN"),
            ("FUN-DFT-006", "đo thính lực đơn âm", "screening", "FUN-DFT"),
            ("LAB-HEM-011", "công thức máu", "screening", "LAB-HEM"),
        ],
    },

    # ─── Hô hấp: Viêm phổi (J18) ───
    "Viêm phổi": {
        "icd10": "J18.9",
        "specialty": "Hô hấp",
        "aliases": ["pneumonia", "viêm phổi cộng đồng", "viêm phổi bệnh viện"],
        "services": [
            ("IMG-XRY-063", "x quang tim phổi thẳng", "screening", "IMG-XRY"),
            ("IMG-CTN-053", "chụp cắt lớp vi tính lồng ngực", "severity", "IMG-CTN"),
            ("LAB-HEM-011", "công thức máu", "screening", "LAB-HEM"),
            ("LAB-HEM-039", "tổng phân tích tế bào máu", "screening", "LAB-HEM"),
            ("LAB-MIC-101", "kháng sinh đồ", "confirmatory", "LAB-MIC"),
            ("LAB-MIC-093", "afb trực tiếp nhuộm ziehl-neelsen", "rule_out", "LAB-MIC"),
            ("LAB-BIO-001", "crp", "monitoring", "LAB-BIO"),
        ],
    },

    # ─── Truyền nhiễm: Lao phổi (A15) ───
    "Lao phổi": {
        "icd10": "A15.0",
        "specialty": "Truyền nhiễm",
        "aliases": ["tuberculosis", "lao", "TB phổi"],
        "services": [
            ("LAB-MIC-093", "afb trực tiếp nhuộm ziehl-neelsen", "confirmatory", "LAB-MIC"),
            ("IMG-XRY-063", "x quang tim phổi thẳng", "screening", "IMG-XRY"),
            ("IMG-CTN-053", "chụp cắt lớp vi tính lồng ngực", "severity", "IMG-CTN"),
            ("LAB-HEM-011", "công thức máu", "screening", "LAB-HEM"),
            ("LAB-MIC-101", "kháng sinh đồ", "confirmatory", "LAB-MIC"),
        ],
    },

    # ─── Truyền nhiễm: Melioidosis (A24) ───
    "Viêm phổi do Burkholderia pseudomallei": {
        "icd10": "A24.1",
        "specialty": "Truyền nhiễm",
        "aliases": ["melioidosis", "Whitmore", "bệnh Whitmore"],
        "services": [
            ("LAB-MIC-101", "kháng sinh đồ", "confirmatory", "LAB-MIC"),
            ("LAB-MIC-093", "afb trực tiếp nhuộm ziehl-neelsen", "rule_out", "LAB-MIC"),
            ("IMG-CTN-053", "chụp cắt lớp vi tính lồng ngực", "severity", "IMG-CTN"),
            ("IMG-XRY-063", "x quang tim phổi thẳng", "screening", "IMG-XRY"),
            ("LAB-HEM-011", "công thức máu", "screening", "LAB-HEM"),
            ("LAB-BIO-001", "crp", "monitoring", "LAB-BIO"),
        ],
    },

    # ─── TMH: Viêm họng cấp (J02) ───
    "Viêm họng cấp": {
        "icd10": "J02.9",
        "specialty": "TMH",
        "aliases": ["viêm họng cấp tính", "pharyngitis"],
        "services": [
            ("END-ENS-001", "nội soi tai mũi họng", "diagnostic", "END-ENS"),
            ("LAB-HEM-011", "công thức máu", "screening", "LAB-HEM"),
            ("LAB-MIC-101", "kháng sinh đồ", "confirmatory", "LAB-MIC"),
            ("GEN-OTH-120", "streptococcus pyogenes aso", "confirmatory", "GEN-OTH"),
        ],
    },

    # ─── TMH: Viêm amidan cấp (J03) ───
    "Viêm amidan cấp": {
        "icd10": "J03.9",
        "specialty": "TMH",
        "aliases": ["viêm amiđan cấp", "viêm amidan"],
        "services": [
            ("END-ENS-001", "nội soi tai mũi họng", "diagnostic", "END-ENS"),
            ("LAB-HEM-011", "công thức máu", "screening", "LAB-HEM"),
            ("LAB-MIC-101", "kháng sinh đồ", "confirmatory", "LAB-MIC"),
            ("GEN-OTH-120", "streptococcus pyogenes aso", "rule_out", "GEN-OTH"),
        ],
    },

    # ─── TMH: Viêm thanh quản cấp (J04.0) ───
    "Viêm thanh quản cấp": {
        "icd10": "J04.0",
        "specialty": "TMH",
        "aliases": ["viêm thanh quản cấp tính", "laryngitis"],
        "services": [
            ("END-ENS-018", "nội soi thanh quản", "diagnostic", "END-ENS"),
            ("END-ENS-001", "nội soi tai mũi họng", "diagnostic", "END-ENS"),
            ("LAB-HEM-011", "công thức máu", "screening", "LAB-HEM"),
            ("IMG-XRY-063", "x quang tim phổi thẳng", "rule_out", "IMG-XRY"),
        ],
    },

    # ─── TMH: Lao thanh quản (A15.5) ───
    "Lao thanh quản": {
        "icd10": "A15.5",
        "specialty": "TMH",
        "aliases": ["lao thanh quản khí quản"],
        "services": [
            ("END-ENS-018", "nội soi thanh quản", "diagnostic", "END-ENS"),
            ("LAB-MIC-093", "afb trực tiếp nhuộm ziehl-neelsen", "confirmatory", "LAB-MIC"),
            ("IMG-XRY-063", "x quang tim phổi thẳng", "screening", "IMG-XRY"),
            ("IMG-CTN-053", "chụp cắt lớp vi tính lồng ngực", "severity", "IMG-CTN"),
            ("LAB-HEM-011", "công thức máu", "screening", "LAB-HEM"),
        ],
    },

    # ─── TMH: Viêm mũi xoang do nấm (J32 + B44) ───
    "Viêm mũi xoang do nấm": {
        "icd10": "J32.9",
        "specialty": "TMH",
        "aliases": ["viêm xoang do nấm", "aspergillosis xoang"],
        "services": [
            ("END-ENS-001", "nội soi tai mũi họng", "diagnostic", "END-ENS"),
            ("IMG-CTN-071", "chụp cắt lớp vi tính", "severity", "IMG-CTN"),
            ("LAB-MIC-101", "kháng sinh đồ", "confirmatory", "LAB-MIC"),
            ("IMG-CTN-004", "chụp cộng hưởng từ mri", "severity", "IMG-CTN"),
        ],
    },

    # ─── TMH: Ung thư vòm mũi họng (C11) ───
    "Ung thư vòm mũi họng": {
        "icd10": "C11",
        "specialty": "TMH",
        "aliases": ["ung thư vòm", "nasopharyngeal carcinoma"],
        "services": [
            ("END-ENS-001", "nội soi tai mũi họng", "diagnostic", "END-ENS"),
            ("IMG-CTN-004", "chụp cộng hưởng từ mri", "severity", "IMG-CTN"),
            ("IMG-CTN-071", "chụp cắt lớp vi tính", "severity", "IMG-CTN"),
            ("PAT-PAT-001", "sinh thiết", "confirmatory", "PAT-PAT"),
            ("LAB-HEM-011", "công thức máu", "screening", "LAB-HEM"),
        ],
    },

    # ─── Huyết học: Bệnh bạch cầu / bệnh máu ───
    "Bệnh bạch cầu cấp": {
        "icd10": "C95.0",
        "specialty": "Huyết học",
        "aliases": ["leukemia cấp", "bệnh máu ác tính"],
        "services": [
            ("LAB-HEM-011", "công thức máu", "screening", "LAB-HEM"),
            ("LAB-HEM-039", "tổng phân tích tế bào máu", "screening", "LAB-HEM"),
            ("END-ENS-001", "nội soi tai mũi họng", "diagnostic", "END-ENS"),
            ("LAB-MIC-101", "kháng sinh đồ", "rule_out", "LAB-MIC"),
        ],
    },

    # ─── Truyền nhiễm: Bạch hầu (A36) ───
    "Bạch hầu": {
        "icd10": "A36.0",
        "specialty": "Truyền nhiễm",
        "aliases": ["diphtheria", "bạch hầu họng"],
        "services": [
            ("END-ENS-001", "nội soi tai mũi họng", "diagnostic", "END-ENS"),
            ("LAB-MIC-101", "kháng sinh đồ", "confirmatory", "LAB-MIC"),
            ("LAB-HEM-011", "công thức máu", "screening", "LAB-HEM"),
        ],
    },

    # ─── Truyền nhiễm: Tăng bạch cầu đơn nhân (B27) ───
    "Tăng bạch cầu đơn nhân nhiễm trùng": {
        "icd10": "B27.9",
        "specialty": "Truyền nhiễm",
        "aliases": ["infectious mononucleosis", "EBV", "Epstein-Barr"],
        "services": [
            ("LAB-HEM-011", "công thức máu", "screening", "LAB-HEM"),
            ("LAB-HEM-039", "tổng phân tích tế bào máu", "screening", "LAB-HEM"),
            ("END-ENS-001", "nội soi tai mũi họng", "diagnostic", "END-ENS"),
            ("GEN-OTH-120", "streptococcus pyogenes aso", "rule_out", "GEN-OTH"),
            ("LAB-MIC-101", "kháng sinh đồ", "rule_out", "LAB-MIC"),
        ],
    },
}


def seed(session):
    stats = {"hypothesis": 0, "expects_service": 0, "disease_entity": 0}

    for disease_name, config in DISEASE_SERVICE_MAP.items():
        icd = config["icd10"]
        specialty = config["specialty"]
        aliases = config.get("aliases", [])
        disease_id = f"disease:{icd.lower().replace('.', '_')}"
        hyp_id = f"hyp:{disease_id}"

        # Ensure DiseaseEntity exists
        session.run("""
            MERGE (d:DiseaseEntity {disease_id: $did})
            ON CREATE SET d.namespace = $ns, d.disease_name = $name,
                          d.icd10 = $icd, d.specialty = $spec, d.aliases = $aliases
            ON MATCH SET d.specialty = $spec, d.aliases = $aliases
        """, did=disease_id, ns=NAMESPACE, name=disease_name,
             icd=icd, spec=specialty, aliases=aliases)

        # Ensure ICDConcept
        session.run("""
            MERGE (i:ICDConcept {icd_code: $icd})
            ON CREATE SET i.namespace = $ns, i.icd_name = $name
        """, icd=icd, ns=NAMESPACE, name=disease_name)
        session.run("""
            MATCH (d:DiseaseEntity {disease_id: $did})
            MATCH (i:ICDConcept {icd_code: $icd})
            MERGE (d)-[:HAS_ICD]->(i)
        """, did=disease_id, icd=icd)

        # Create DiseaseHypothesis
        session.run("""
            MERGE (h:DiseaseHypothesis {hypothesis_id: $hid})
            ON CREATE SET h.namespace = $ns, h.disease_id = $did,
                          h.disease_name = $name, h.icd10 = $icd,
                          h.specialty = $spec
            ON MATCH SET h.specialty = $spec
        """, hid=hyp_id, ns=NAMESPACE, did=disease_id,
             name=disease_name, icd=icd, spec=specialty)
        stats["hypothesis"] += 1

        # Link hypothesis → disease
        session.run("""
            MATCH (h:DiseaseHypothesis {hypothesis_id: $hid})
            MATCH (d:DiseaseEntity {disease_id: $did})
            MERGE (h)-[:HYPOTHESIS_FOR_DISEASE]->(d)
        """, hid=hyp_id, did=disease_id)

        stats["disease_entity"] += 1

        # DISEASE_EXPECTS_SERVICE with role
        for svc_code, svc_name, role, cat_code in config["services"]:
            session.run("""
                MATCH (h:DiseaseHypothesis {hypothesis_id: $hid})
                MATCH (s:ProtocolService {service_code: $sc})
                MERGE (h)-[r:DISEASE_EXPECTS_SERVICE]->(s)
                ON CREATE SET r.role = $role, r.category_code = $cc,
                              r.service_name_hint = $sname
                ON MATCH SET r.role = $role
            """, hid=hyp_id, sc=svc_code, role=role, cc=cat_code, sname=svc_name)
            # Also try CIService if ProtocolService miss
            session.run("""
                MATCH (h:DiseaseHypothesis {hypothesis_id: $hid})
                MATCH (s:CIService {service_code: $sc})
                WHERE NOT EXISTS {
                    MATCH (h)-[:DISEASE_EXPECTS_SERVICE]->(:ProtocolService {service_code: $sc})
                }
                MERGE (h)-[r:DISEASE_EXPECTS_SERVICE]->(s)
                ON CREATE SET r.role = $role, r.category_code = $cc,
                              r.service_name_hint = $sname
                ON MATCH SET r.role = $role
            """, hid=hyp_id, sc=svc_code, role=role, cc=cat_code, sname=svc_name)
            stats["expects_service"] += 1

    return stats


def verify(session):
    """Print verification counts."""
    print("\n  Verification:")
    r = session.run("MATCH (h:DiseaseHypothesis) RETURN count(h) AS c").single()
    print(f"    DiseaseHypothesis: {r['c']}")
    r = session.run("MATCH ()-[r:DISEASE_EXPECTS_SERVICE]->() RETURN count(r) AS c").single()
    print(f"    DISEASE_EXPECTS_SERVICE: {r['c']}")
    r = session.run("MATCH (d:DiseaseEntity) RETURN count(d) AS c").single()
    print(f"    DiseaseEntity: {r['c']}")

    print("\n  Per-disease expected services:")
    for record in session.run("""
        MATCH (h:DiseaseHypothesis)-[r:DISEASE_EXPECTS_SERVICE]->(s)
        WHERE s:ProtocolService OR s:CIService
        WITH h.disease_name AS disease, h.specialty AS spec,
             collect({code: s.service_code, name: coalesce(s.service_name, s.canonical_name), role: r.role}) AS services
        RETURN disease, spec, size(services) AS svc_count, services
        ORDER BY disease
    """):
        disease = record["disease"]
        spec = record["spec"]
        svc_count = record["svc_count"]
        services = record["services"]
        print(f"\n    {disease} ({spec}) — {svc_count} services:")
        for s in services:
            role = s['role'] or '?'
            code = s['code'] or '?'
            name = s['name'] or '?'
            print(f"      [{role:12s}] {code} — {name}")


if __name__ == "__main__":
    print("=" * 70)
    print("  Seeding Disease → ExpectedService with roles")
    print("=" * 70)

    with driver.session() as session:
        stats = seed(session)
        print(f"\n  Created: {stats}")
        verify(session)

    driver.close()
    print(f"\n{'=' * 70}")
    print("  DONE")
    print(f"{'=' * 70}")
