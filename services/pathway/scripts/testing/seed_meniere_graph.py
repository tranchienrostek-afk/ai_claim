"""
Seed H81.0 Bệnh Meniere into Neo4j graph.

Creates:
- Disease node for H81.0 with clinical metadata
- LabTest/Procedure nodes for PTA, ENG, MRI, abdominal US
- INDICATION_FOR edges (PTA, ENG → Meniere)
- RULE_OUT_FOR edges (MRI → acoustic neuroma, central causes)
- CONTRA_INDICATES edges (abdominal US — irrelevant to Meniere)
- ASSERTION_INDICATES_SERVICE edges (Disease → CIService codes)
- BenefitInterpretation node for MRI inpatient/outpatient rule under TIN-PNC

Usage:
    set PYTHONIOENCODING=utf-8 && python -X utf8 scripts/testing/seed_meniere_graph.py [--clear]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List

NOTEBOOKLM_DIR = Path(__file__).resolve().parents[2]
if str(NOTEBOOKLM_DIR) not in sys.path:
    sys.path.insert(0, str(NOTEBOOKLM_DIR))

try:
    from neo4j import GraphDatabase
except ImportError:
    print("pip install neo4j")
    sys.exit(1)

NEO4J_URI = "bolt://localhost:7688"
NEO4J_AUTH = ("neo4j", "password123")
NAMESPACE = "benchmark_clinical_v1"

# ── Disease definitions ──────────────────────────────────────────────

DISEASES = [
    {
        "icd10": "H81.0",
        "name": "Bệnh Meniere",
        "name_en": "Meniere disease",
        "aliases": ["Meniere", "benh meniere", "hội chứng Meniere"],
        "triad": "chóng mặt quay cuồng + ù tai + nghe kém một bên",
        "specialty": "ENT",
        "description": "Bệnh tai trong do tích tụ nội dịch gây chóng mặt kịch phát, ù tai tiếng trầm, nghe kém dao động và cảm giác đầy bưng tai.",
    },
    {
        "icd10": "D33.3",
        "name": "U dây thần kinh VIII",
        "name_en": "Acoustic neuroma / Vestibular schwannoma",
        "aliases": ["acoustic neuroma", "vestibular schwannoma", "u tu cung nghe"],
        "specialty": "ENT/Neurosurgery",
        "description": "U lành tính dây thần kinh tiền đình ốc tai, cần loại trừ khi nghe kém một bên tiến triển.",
    },
    {
        "icd10": "J18.9",
        "name": "Viêm phổi cộng đồng",
        "name_en": "Community-acquired pneumonia",
        "aliases": ["viem phoi", "pneumonia", "CAP", "viêm phổi"],
        "specialty": "Internal Medicine/Pulmonology",
        "description": "Viêm phổi mắc phải ở cộng đồng, thường do vi khuẩn (S. pneumoniae, H. influenzae). Triệu chứng: ho đờm, sốt, khó thở, đau ngực.",
    },
    {
        "icd10": "J20.9",
        "name": "Viêm phế quản cấp",
        "name_en": "Acute bronchitis",
        "aliases": ["viem phe quan", "bronchitis"],
        "specialty": "Internal Medicine",
        "description": "Viêm phế quản cấp tính, thường do virus, cần phân biệt với viêm phổi.",
    },
]

# ── Service expectations for H81.0 ──────────────────────────────────

SERVICE_EXPECTATIONS = [
    {
        "test_name": "Đo thính lực đơn âm (PTA)",
        "test_name_en": "Pure Tone Audiometry",
        "label": "LabTest",
        "role": "diagnostic",
        "disease_icd": "H81.0",
        "edge_type": "INDICATION_FOR",
        "ci_service_code": "FUN-DFT-006",
        "reasoning": "Tiêu chuẩn vàng đánh giá nghe kém sensorineural một bên — bắt buộc theo AAO-HNS để chẩn đoán Meniere.",
        "confidence": 1.0,
    },
    {
        "test_name": "Điện động nhãn đồ (ENG)",
        "test_name_en": "Electronystagmography",
        "label": "LabTest",
        "role": "diagnostic",
        "disease_icd": "H81.0",
        "edge_type": "INDICATION_FOR",
        "ci_service_code": None,
        "reasoning": "Đánh giá chức năng đội, phân biệt nguyên nhân chóng mặt nội biên vs trung ương.",
        "confidence": 0.95,
    },
    {
        "test_name": "MRI sọ não có tiêm thuốc",
        "test_name_en": "Brain MRI with contrast",
        "label": "LabTest",
        "role": "rule_out",
        "disease_icd": "H81.0",
        "edge_type": "RULE_OUT_FOR",
        "rule_out_target_icd": "D33.3",
        "ci_service_code": "IMG-CTN-013",
        "reasoning": "Loại trừ u dây thần kinh VIII (acoustic neuroma) và nguyên nhân chóng mặt trung ương. Bắt buộc khi nghe kém một bên.",
        "confidence": 0.9,
    },
    {
        "test_name": "Siêu âm ổ bụng tổng quát",
        "test_name_en": "Abdominal ultrasound",
        "label": "LabTest",
        "role": "irrelevant",
        "disease_icd": "H81.0",
        "edge_type": "CONTRA_INDICATES",
        "ci_service_code": None,
        "reasoning": "Không liên quan đến bệnh lý tai-tiền đình. Không có chỉ định y khoa cho siêu âm bụng trong chẩn đoán/điều trị Meniere.",
        "confidence": 1.0,
    },
    # ── Pneumonia (J18.9) service expectations ──
    {
        "test_name": "Công thức máu (CBC)",
        "test_name_en": "Complete blood count",
        "label": "LabTest",
        "role": "diagnostic",
        "disease_icd": "J18.9",
        "edge_type": "INDICATION_FOR",
        "ci_service_code": None,
        "reasoning": "Bạch cầu tăng + neutrophil ưu thế gợi ý nhiễm trùng vi khuẩn. Xét nghiệm cơ bản bắt buộc cho viêm phổi nghi ngờ.",
        "confidence": 1.0,
    },
    {
        "test_name": "CRP (C-Reactive Protein)",
        "test_name_en": "C-Reactive Protein",
        "label": "LabTest",
        "role": "diagnostic",
        "disease_icd": "J18.9",
        "edge_type": "INDICATION_FOR",
        "ci_service_code": None,
        "reasoning": "Marker viêm hỗ trợ chẩn đoán viêm phổi và theo dõi đáp ứng điều trị. CRP > 100 mg/L gợi ý nhiễm khuẩn.",
        "confidence": 0.95,
    },
    {
        "test_name": "X-quang ngực thẳng",
        "test_name_en": "Chest X-ray PA",
        "label": "LabTest",
        "role": "confirmatory",
        "disease_icd": "J18.9",
        "edge_type": "INDICATION_FOR",
        "ci_service_code": None,
        "reasoning": "Tiêu chuẩn vàng xác nhận viêm phổi: thâm nhiễm nhu mô phổi, đông đặc thùy, hoặc tràn dịch cạnh phổi.",
        "confidence": 1.0,
    },
    {
        "test_name": "Cấy đờm và kháng sinh đồ",
        "test_name_en": "Sputum culture and sensitivity",
        "label": "LabTest",
        "role": "diagnostic",
        "disease_icd": "J18.9",
        "edge_type": "INDICATION_FOR",
        "ci_service_code": None,
        "reasoning": "Xác định tác nhân gây bệnh và kháng sinh nhạy cảm. Bắt buộc khi viêm phổi không đáp ứng kháng sinh ban đầu hoặc nghi kháng thuốc.",
        "confidence": 0.9,
    },
    {
        "test_name": "Xét nghiệm chức năng gan (AST, ALT)",
        "test_name_en": "Liver function test (AST, ALT)",
        "label": "LabTest",
        "role": "screening_irrelevant",
        "disease_icd": "J18.9",
        "edge_type": "CONTRA_INDICATES",
        "ci_service_code": None,
        "reasoning": "Chức năng gan không liên quan đến chẩn đoán/điều trị viêm phổi cộng đồng ở bệnh nhân không có tiền sử gan. Đây là xét nghiệm tầm soát, không phải chỉ định y khoa.",
        "confidence": 1.0,
    },
]

# ── Benefit interpretation rule for MRI under TIN-PNC ────────────────

BENEFIT_INTERPRETATIONS = [
    {
        "rule_id": "INTERP-TIN-PNC-MRI-001",
        "contract_id": "TIN-PNC",
        "service_type": "MRI/CT/PET",
        "conflict_clauses": ["BEN-TIN-PNC-22", "BEN-TIN-PNC-48"],
        "resolution": "partial_pay",
        "reasoning": (
            "BEN-TIN-PNC-22 giới hạn MRI/CT/PET vào bối cảnh nội trú ('phải là một phần của chi phí Điều trị nội trú'). "
            "BEN-TIN-PNC-48 bao gồm 'chẩn đoán hình ảnh cần thiết cho chẩn đoán và điều trị bệnh' ở ngoại trú nhưng không đặc tên MRI. "
            "Nguyên tắc: điều khoản cụ thể (BEN-TIN-PNC-22) ưu tiên hơn điều khoản chung (BEN-TIN-PNC-48). "
            "Tuy nhiên, nếu MRI được bác sĩ chỉ định và cần thiết về y khoa (rule-out), ngoại trú có thể chấp nhận nhưng phải "
            "cắt theo hạn mức (1.400.000 VNĐ/lần khám) → partial_pay thay vì deny toàn bộ."
        ),
        "conditions": [
            "care_type == ngoai_tru",
            "service cần thiết y khoa (rule-out hoặc diagnostic)",
            "tổng chi phí vượt hạn mức ngoại trú",
        ],
    },
]


def seed(driver: Any, clear: bool = False) -> Dict[str, int]:
    counts: Dict[str, int] = {
        "diseases": 0,
        "tests": 0,
        "indication_for": 0,
        "rule_out_for": 0,
        "contra_indicates": 0,
        "assertion_indicates_service": 0,
        "benefit_interpretations": 0,
    }

    with driver.session() as session:
        if clear:
            session.run(
                "MATCH (n) WHERE n.namespace = $ns DETACH DELETE n",
                ns=NAMESPACE,
            )
            session.run(
                "MATCH ()-[r]->() WHERE r.namespace = $ns DELETE r",
                ns=NAMESPACE,
            )
            print(f"Cleared namespace {NAMESPACE}")

        # Create Disease nodes
        for disease in DISEASES:
            session.run(
                """
                MERGE (d:Disease {icd10: $icd10})
                SET d.name = $name,
                    d.name_en = $name_en,
                    d.aliases = $aliases,
                    d.specialty = $specialty,
                    d.description = $description,
                    d.namespace = $namespace
                """,
                icd10=disease["icd10"],
                name=disease["name"],
                name_en=disease["name_en"],
                aliases=disease.get("aliases", []),
                specialty=disease["specialty"],
                description=disease["description"],
                namespace=NAMESPACE,
            )
            counts["diseases"] += 1
            print(f"  Disease: {disease['icd10']} {disease['name']}")

        # Create service expectations
        for svc in SERVICE_EXPECTATIONS:
            # Create LabTest/Procedure node
            label = svc["label"]
            session.run(
                f"""
                MERGE (t:{label} {{name: $name, namespace: $namespace}})
                SET t.name_en = $name_en,
                    t.role = $role,
                    t.ci_service_code = $ci_code,
                    t.reasoning = $reasoning,
                    t.confidence = $confidence
                """,
                name=svc["test_name"],
                name_en=svc["test_name_en"],
                role=svc["role"],
                ci_code=svc.get("ci_service_code"),
                reasoning=svc["reasoning"],
                confidence=svc["confidence"],
                namespace=NAMESPACE,
            )
            counts["tests"] += 1

            # Create edge to disease
            edge_type = svc["edge_type"]
            target_icd = svc.get("rule_out_target_icd", svc["disease_icd"])
            session.run(
                f"""
                MATCH (t:{label} {{name: $test_name, namespace: $namespace}})
                MATCH (d:Disease {{icd10: $icd10}})
                MERGE (t)-[r:{edge_type}]->(d)
                SET r.role = $role,
                    r.reasoning = $reasoning,
                    r.confidence = $confidence,
                    r.namespace = $namespace,
                    r.source = 'benchmark_seed'
                """,
                test_name=svc["test_name"],
                icd10=target_icd,
                role=svc["role"],
                reasoning=svc["reasoning"],
                confidence=svc["confidence"],
                namespace=NAMESPACE,
            )
            counts[edge_type.lower()] = counts.get(edge_type.lower(), 0) + 1
            print(f"  {svc['test_name']} --{edge_type}--> {target_icd}")

            # Create ASSERTION_INDICATES_SERVICE edge (Disease → CIService)
            if svc.get("ci_service_code") and svc["edge_type"] != "CONTRA_INDICATES":
                result = session.run(
                    """
                    MATCH (d:Disease {icd10: $icd10})
                    MATCH (s:CIService {service_code: $code})
                    MERGE (d)-[r:ASSERTION_INDICATES_SERVICE]->(s)
                    SET r.role = $role,
                        r.reasoning = $reasoning,
                        r.confidence = $confidence,
                        r.namespace = $namespace,
                        r.source = 'benchmark_seed'
                    RETURN count(r) as cnt
                    """,
                    icd10=svc["disease_icd"],
                    code=svc["ci_service_code"],
                    role=svc["role"],
                    reasoning=svc["reasoning"],
                    confidence=svc["confidence"],
                    namespace=NAMESPACE,
                )
                cnt = result.single()["cnt"]
                if cnt:
                    counts["assertion_indicates_service"] += cnt
                    print(f"  {svc['disease_icd']} --ASSERTION_INDICATES_SERVICE--> {svc['ci_service_code']}")

        # Create BenefitInterpretation nodes
        for interp in BENEFIT_INTERPRETATIONS:
            session.run(
                """
                MERGE (bi:BenefitInterpretation {rule_id: $rule_id})
                SET bi.contract_id = $contract_id,
                    bi.service_type = $service_type,
                    bi.conflict_clauses = $conflict_clauses,
                    bi.resolution = $resolution,
                    bi.reasoning = $reasoning,
                    bi.conditions = $conditions,
                    bi.namespace = $namespace
                """,
                rule_id=interp["rule_id"],
                contract_id=interp["contract_id"],
                service_type=interp["service_type"],
                conflict_clauses=interp["conflict_clauses"],
                resolution=interp["resolution"],
                reasoning=interp["reasoning"],
                conditions=interp["conditions"],
                namespace=NAMESPACE,
            )
            # Link to InsuranceContract
            session.run(
                """
                MATCH (bi:BenefitInterpretation {rule_id: $rule_id})
                MATCH (c:InsuranceContract {contract_id: $contract_id})
                MERGE (c)-[r:HAS_INTERPRETATION]->(bi)
                SET r.namespace = $namespace
                """,
                rule_id=interp["rule_id"],
                contract_id=interp["contract_id"],
                namespace=NAMESPACE,
            )
            # Link to relevant Benefit nodes
            for clause_id in interp["conflict_clauses"]:
                session.run(
                    """
                    MATCH (bi:BenefitInterpretation {rule_id: $rule_id})
                    MATCH (b:Benefit {entry_id: $entry_id})
                    MERGE (bi)-[r:INTERPRETS]->(b)
                    SET r.namespace = $namespace
                    """,
                    rule_id=interp["rule_id"],
                    entry_id=clause_id,
                    namespace=NAMESPACE,
                )
            counts["benefit_interpretations"] += 1
            print(f"  BenefitInterpretation: {interp['rule_id']} → {interp['resolution']}")

    return counts


def verify(driver: Any) -> None:
    with driver.session() as session:
        r = session.run(
            "MATCH (d:Disease {icd10: 'H81.0'})-[r]-() RETURN type(r) as rel, count(r) as cnt"
        )
        print("\n=== H81.0 edges ===")
        for rec in r:
            print(f"  {rec['rel']}: {rec['cnt']}")

        r = session.run(
            "MATCH ()-[r:ASSERTION_INDICATES_SERVICE]->() RETURN count(r) as cnt"
        )
        print(f"\nASSERTION_INDICATES_SERVICE total: {r.single()['cnt']}")

        r = session.run(
            "MATCH (bi:BenefitInterpretation) RETURN bi.rule_id, bi.resolution"
        )
        print("\n=== BenefitInterpretation ===")
        for rec in r:
            print(f"  {rec['bi.rule_id']}: {rec['bi.resolution']}")

        # Full trace test: Disease → Service → CanonicalService
        r = session.run(
            """
            MATCH (d:Disease {icd10: 'H81.0'})-[:ASSERTION_INDICATES_SERVICE]->(s:CIService)
            OPTIONAL MATCH (s)-[:MAPS_TO_CANONICAL]->(cs:CanonicalService)
            RETURN d.name, s.service_code, s.service_name, cs.service_name as canonical
            """
        )
        print("\n=== Disease → CIService → CanonicalService trace ===")
        for rec in r:
            print(f"  {rec['d.name']} → {rec['s.service_code']} ({rec['s.service_name']}) → {rec['canonical']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed H81.0 Meniere into Neo4j")
    parser.add_argument("--clear", action="store_true", help="Clear existing benchmark_clinical_v1 namespace first")
    args = parser.parse_args()

    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    try:
        counts = seed(driver, clear=args.clear)
        print(f"\nSeeded: {counts}")
        verify(driver)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
