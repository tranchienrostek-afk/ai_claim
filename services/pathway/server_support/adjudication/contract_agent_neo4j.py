"""Neo4j-based Contract Agent for insurance coverage evaluation."""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from neo4j import GraphDatabase

from .models import AgentVerdict, Decision, EvidenceItem, ServiceLineInput

logger = logging.getLogger(__name__)

NEO4J_URI = os.getenv("NEO4J_URI", os.getenv("neo4j_uri", "bolt://localhost:7688"))
NEO4J_USER = os.getenv("NEO4J_USER", os.getenv("neo4j_user", "neo4j"))
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", os.getenv("neo4j_password", "password123"))
INSURANCE_NAMESPACE = "claims_insights_insurance_v1"


class ContractAgentNeo4j:
    """Neo4j-powered contract agent for insurance coverage evaluation."""

    agent_name: str = "contract"

    def __init__(self, uri: str = NEO4J_URI, user: str = NEO4J_USER, password: str = NEO4J_PASSWORD):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self._cache: dict[str, Any] = {}

    def close(self):
        if self.driver:
            self.driver.close()

    def _run_query(self, query: str, params: dict[str, Any] | None = None) -> list[dict]:
        with self.driver.session() as session:
            result = session.run(query, params or {})
            return [dict(record) for record in result]

    def _get_contract_info(self, contract_id: str) -> dict[str, Any]:
        cache_key = f"contract_{contract_id}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        result = self._run_query(
            """
            MATCH (c:InsuranceContract {contract_id: $contract_id})
            WHERE coalesce(c.namespace, '') = $namespace
            OPTIONAL MATCH (i:Insurer)-[:ISSUES]->(c)
            RETURN c.contract_id AS contract_id,
                   c.product_name AS product_name,
                   c.insurer AS insurer,
                   c.mode AS mode,
                   c.requires_preauth AS requires_preauth,
                   c.positive_result_required AS positive_result_required,
                   c.paid_ratio_pct AS paid_ratio_pct
            LIMIT 1
            """,
            {"contract_id": contract_id, "namespace": INSURANCE_NAMESPACE},
        )
        contract = result[0] if result else {}
        if contract:
            self._cache[cache_key] = contract
        return contract

    def _resolve_medical_context(
        self,
        line: ServiceLineInput,
        service_info: dict[str, Any],
    ) -> dict[str, Any]:
        context = service_info.get("medical_context") or {}
        active_diseases = context.get("active_diseases") or service_info.get("active_diseases") or []
        disease_rows: list[dict[str, Any]] = []

        for item in active_diseases[:8]:
            disease_id = item.get("disease_id") or ""
            disease_name = item.get("disease_name") or ""
            icd10 = item.get("icd10") or line.primary_icd or ""
            if disease_id or disease_name or icd10:
                disease_rows.append(
                    {
                        "disease_id": disease_id,
                        "disease_name": disease_name,
                        "icd10": icd10,
                    }
                )

        if not disease_rows and line.primary_icd:
            rows = self._run_query(
                """
                MATCH (d:CIDisease {namespace:$namespace})
                WHERE d.icd10 = $icd OR d.disease_id = $disease_id
                RETURN d.disease_id AS disease_id,
                       d.disease_name AS disease_name,
                       d.icd10 AS icd10
                LIMIT 5
                """,
                {"namespace": "claims_insights_explorer_v1", "icd": line.primary_icd, "disease_id": f"disease:{line.primary_icd}"},
            )
            disease_rows.extend(rows)

        if not disease_rows and line.diagnosis_text:
            rows = self._run_query(
                """
                MATCH (d:CIDisease {namespace:$namespace})
                WHERE toLower(coalesce(d.disease_name, '')) CONTAINS toLower($query)
                RETURN d.disease_id AS disease_id,
                       d.disease_name AS disease_name,
                       d.icd10 AS icd10
                ORDER BY coalesce(d.case_count, 0) DESC
                LIMIT 5
                """,
                {"namespace": "claims_insights_explorer_v1", "query": line.diagnosis_text},
            )
            disease_rows.extend(rows)

        disease_ids = []
        seen_disease_ids: set[str] = set()
        for row in disease_rows:
            disease_id = str(row.get("disease_id") or "").strip()
            if not disease_id or disease_id in seen_disease_ids:
                continue
            seen_disease_ids.add(disease_id)
            disease_ids.append(
                {
                    "disease_id": disease_id,
                    "disease_name": row.get("disease_name", ""),
                    "icd10": row.get("icd10", ""),
                }
            )

        raw_signs = []
        raw_signs.extend(context.get("input_signs") or [])
        raw_signs.extend(line.symptoms or [])
        seen_sign_ids: set[str] = set()
        sign_rows: list[dict[str, Any]] = []
        for sign_text in raw_signs[:12]:
            if not str(sign_text or "").strip():
                continue
            rows = self._run_query(
                """
                MATCH (s:CISign)
                WHERE toLower(coalesce(s.text, '')) = toLower($query)
                   OR toLower(coalesce(s.canonical_label, '')) = toLower($query)
                   OR toLower(coalesce(s.text, '')) CONTAINS toLower($query)
                RETURN s.sign_id AS sign_id,
                       coalesce(s.canonical_label, s.text) AS sign_label
                ORDER BY size(coalesce(s.text, '')) ASC
                LIMIT 3
                """,
                {"query": str(sign_text)},
            )
            for row in rows:
                sign_id = str(row.get("sign_id") or "").strip()
                if not sign_id or sign_id in seen_sign_ids:
                    continue
                seen_sign_ids.add(sign_id)
                sign_rows.append({"sign_id": sign_id, "sign_label": row.get("sign_label", "")})

        return {
            "diseases": disease_ids[:6],
            "signs": sign_rows[:8],
        }

    def _get_benefits_for_service(
        self,
        contract_id: str,
        service_name: str,
        service_code: str = "",
        category_code: str = "",
    ) -> list[dict[str, Any]]:
        if service_code:
            direct_result = self._run_query(
                """
                MATCH (svc:CIService {service_code: $service_code})
                MATCH (svc)-[r:SUPPORTED_BY_CONTRACT_BENEFIT {contract_id: $contract_id}]->(b:Benefit)
                WHERE coalesce(r.namespace, '') = $namespace
                  AND coalesce(b.namespace, '') = $namespace
                OPTIONAL MATCH (b)-[:INTERPRETED_AS]->(bi:BenefitInterpretation)
                RETURN DISTINCT
                       b.entry_id AS entry_id,
                       b.entry_label AS entry_label,
                       b.major_section AS major_section,
                       b.subsection AS subsection,
                       b.canonical_name AS canonical_name,
                       bi.entry_id AS interpretation_entry_id,
                       bi.canonical_name AS interpretation_name,
                       r.support_rows AS support_rows,
                       r.claim_count AS claim_count,
                       r.total_detail_amount_vnd AS total_detail_amount_vnd,
                       r.status_distribution_json AS status_distribution_json,
                       'direct_service_support' AS match_source
                ORDER BY coalesce(r.support_rows, 0) DESC, coalesce(r.claim_count, 0) DESC, b.entry_label
                LIMIT 20
                """,
                {
                    "contract_id": contract_id,
                    "service_code": service_code,
                    "namespace": INSURANCE_NAMESPACE,
                },
            )
            if direct_result:
                return direct_result

        return self._run_query(
            """
            MATCH (c:InsuranceContract {contract_id: $contract_id})-[:HAS_BENEFIT]->(b:Benefit)
            WHERE coalesce(c.namespace, '') = $namespace
              AND coalesce(b.namespace, '') = $namespace
            OPTIONAL MATCH (b)-[:INTERPRETED_AS]->(bi:BenefitInterpretation)
            WITH b, bi,
                 CASE
                    WHEN toLower(coalesce(b.entry_label, '')) = toLower($service_name) THEN 4
                    WHEN toLower(coalesce(b.canonical_name, '')) = toLower($service_name) THEN 4
                    WHEN toLower(coalesce(b.entry_label, '')) CONTAINS toLower($service_name) THEN 3
                    WHEN toLower(coalesce(b.canonical_name, '')) CONTAINS toLower($service_name) THEN 3
                    WHEN toLower(coalesce(bi.canonical_name, '')) CONTAINS toLower($service_name) THEN 2
                    WHEN $category <> '' AND toLower(coalesce(b.major_section, '')) CONTAINS toLower($category) THEN 1
                    ELSE 0
                 END AS match_score
            WHERE match_score > 0
            RETURN b.entry_id AS entry_id,
                   b.entry_label AS entry_label,
                   b.major_section AS major_section,
                   b.subsection AS subsection,
                   b.canonical_name AS canonical_name,
                   bi.entry_id AS interpretation_entry_id,
                   bi.canonical_name AS interpretation_name,
                   match_score AS match_score,
                   'text_fallback' AS match_source
            ORDER BY match_score DESC, b.entry_label
            LIMIT 20
            """,
            {
                "contract_id": contract_id,
                "service_name": service_name,
                "category": category_code or service_name[:50],
                "namespace": INSURANCE_NAMESPACE,
            },
        )

    def _get_plans_for_contract(self, contract_id: str) -> list[dict[str, Any]]:
        cache_key = f"plans_{contract_id}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        result = self._run_query(
            """
            MATCH (c:InsuranceContract {contract_id: $contract_id})-[:COVERS_PLAN]->(p:ContractPlan)
            WHERE coalesce(c.namespace, '') = $namespace
              AND coalesce(p.namespace, '') = $namespace
            RETURN p.plan_id AS plan_id,
                   p.name AS name,
                   p.contract_id AS contract_id
            ORDER BY p.name
            """,
            {"contract_id": contract_id, "namespace": INSURANCE_NAMESPACE},
        )
        self._cache[cache_key] = result
        return result

    def _check_benefit_coverage(
        self,
        contract_id: str,
        benefit_label: str,
        patient_plan: str = "",
    ) -> dict[str, Any]:
        if not patient_plan:
            result = self._run_query(
                """
                MATCH (c:InsuranceContract {contract_id: $contract_id})-[:HAS_BENEFIT]->(b:Benefit)
                WHERE coalesce(c.namespace, '') = $namespace
                  AND coalesce(b.namespace, '') = $namespace
                  AND b.entry_label = $benefit_label
                RETURN b.entry_id AS entry_id, 'exists' AS coverage_status
                LIMIT 1
                """,
                {
                    "contract_id": contract_id,
                    "benefit_label": benefit_label,
                    "namespace": INSURANCE_NAMESPACE,
                },
            )
            return result[0] if result else {}

        result = self._run_query(
            """
            MATCH (c:InsuranceContract {contract_id: $contract_id})
            MATCH (c)-[:COVERS_PLAN]->(p:ContractPlan {name: $plan_name})-[r:COVERS_BENEFIT]->(b:Benefit)
            WHERE coalesce(c.namespace, '') = $namespace
              AND coalesce(p.namespace, '') = $namespace
              AND coalesce(b.namespace, '') = $namespace
              AND (
                    b.entry_label = $benefit_label
                 OR b.entry_id = $benefit_label
                 OR b.canonical_name = $benefit_label
              )
            RETURN b.entry_label AS benefit_label,
                   p.name AS plan_name,
                   r.coverage AS coverage,
                   'covered' AS coverage_status
            LIMIT 1
            """,
            {
                "contract_id": contract_id,
                "plan_name": patient_plan,
                "benefit_label": benefit_label,
                "namespace": INSURANCE_NAMESPACE,
            },
        )
        return result[0] if result else {}

    def _search_exclusions(
        self,
        contract_id: str,
        service_name: str,
        service_code: str = "",
        category_code: str = "",
    ) -> list[dict[str, Any]]:
        if service_code:
            contract_direct_matches = self._run_query(
                """
                MATCH (svc:CIService {service_code: $service_code})
                MATCH (svc)-[r:EXCLUDED_BY_CONTRACT {contract_id: $contract_id}]->(e:Exclusion)
                WHERE coalesce(r.namespace, '') = $namespace
                  AND coalesce(e.namespace, '') = $namespace
                OPTIONAL MATCH (e)-[:HAS_REASON]->(er:ExclusionReason)
                RETURN DISTINCT
                       e.code AS code,
                       e.group AS group,
                       e.reason AS reason,
                       er.text AS exclusion_reason_text,
                       coalesce(e.usage_total_rows, 0) AS usage_total_rows,
                       e.usage_gap_vnd AS usage_gap_vnd,
                       coalesce(r.rows, 0) AS matched_rows,
                       r.reason_texts AS reason_texts,
                       '' AS contract_distribution_json,
                       r.rule_distribution_json AS rule_distribution_json,
                       'direct_contract_service_exclusion' AS match_source
                ORDER BY matched_rows DESC, usage_total_rows DESC
                LIMIT 10
                """,
                {
                    "contract_id": contract_id,
                    "service_code": service_code,
                    "namespace": INSURANCE_NAMESPACE,
                },
            )
            if contract_direct_matches:
                return contract_direct_matches

            direct_matches = self._run_query(
                """
                MATCH (svc:CIService {service_code: $service_code})
                MATCH (svc)-[r:EXCLUDED_BY]->(e:Exclusion)
                WHERE coalesce(r.namespace, '') = $namespace
                  AND coalesce(e.namespace, '') = $namespace
                OPTIONAL MATCH (e)-[:HAS_REASON]->(er:ExclusionReason)
                RETURN DISTINCT
                       e.code AS code,
                       e.group AS group,
                       e.reason AS reason,
                       er.text AS exclusion_reason_text,
                       coalesce(e.usage_total_rows, 0) AS usage_total_rows,
                       e.usage_gap_vnd AS usage_gap_vnd,
                       coalesce(r.rows, 0) AS matched_rows,
                       r.reason_texts AS reason_texts,
                       r.contract_distribution_json AS contract_distribution_json,
                       r.rule_distribution_json AS rule_distribution_json,
                       'direct_service_exclusion' AS match_source
                ORDER BY matched_rows DESC, usage_total_rows DESC
                LIMIT 10
                """,
                {"service_code": service_code, "namespace": INSURANCE_NAMESPACE},
            )
            if direct_matches:
                return direct_matches

            historical_matches = self._run_query(
                """
                MATCH (svc:CIService {service_code: $service_code})-[r:HISTORICALLY_EXCLUDED]->(ep:ExclusionPattern)
                WHERE coalesce(r.namespace, '') = $namespace
                  AND coalesce(ep.namespace, '') = $namespace
                OPTIONAL MATCH (ep)-[:MATCHES_EXCLUSION]->(e:Exclusion)
                OPTIONAL MATCH (e)-[:HAS_REASON]->(er:ExclusionReason)
                RETURN DISTINCT
                       coalesce(e.code, ep.exclusion_code) AS code,
                       e.group AS group,
                       coalesce(e.reason, ep.reason) AS reason,
                       er.text AS exclusion_reason_text,
                       coalesce(e.usage_total_rows, ep.total_rows) AS usage_total_rows,
                       e.usage_gap_vnd AS usage_gap_vnd,
                       coalesce(r.rows, 0) AS matched_rows,
                       '' AS reason_texts,
                       '' AS contract_distribution_json,
                       '' AS rule_distribution_json,
                       'historical_service_exclusion' AS match_source
                ORDER BY matched_rows DESC, usage_total_rows DESC
                LIMIT 10
                """,
                {"service_code": service_code, "namespace": INSURANCE_NAMESPACE},
            )
            if historical_matches:
                return historical_matches

        keywords = self._extract_keywords(service_name, category_code)
        return self._run_query(
            """
            MATCH (c:InsuranceContract {contract_id: $contract_id})-[:HAS_EXCLUSION]->(e:Exclusion)
            WHERE coalesce(c.namespace, '') = $namespace
              AND coalesce(e.namespace, '') = $namespace
            OPTIONAL MATCH (e)-[:HAS_REASON]->(er:ExclusionReason)
            WITH e, er
            WHERE (
                   coalesce(e.usage_total_rows, 0) > 0
                OR er.text IS NOT NULL
                OR e.reason IS NOT NULL
              )
              AND any(keyword IN $keywords WHERE toLower(coalesce(er.text, '')) CONTAINS keyword
                  OR toLower(coalesce(e.reason, '')) CONTAINS keyword
                  OR toLower(coalesce(e.group, '')) CONTAINS keyword)
            RETURN e.code AS code,
                   e.group AS group,
                   e.reason AS reason,
                   er.text AS exclusion_reason_text,
                   coalesce(e.usage_total_rows, 0) AS usage_total_rows,
                   e.usage_gap_vnd AS usage_gap_vnd,
                   0 AS matched_rows,
                   [] AS reason_texts,
                   '' AS contract_distribution_json,
                   '' AS rule_distribution_json,
                   'keyword_contract_exclusion' AS match_source
            ORDER BY usage_total_rows DESC
            LIMIT 10
            """,
            {
                "contract_id": contract_id,
                "keywords": keywords,
                "namespace": INSURANCE_NAMESPACE,
            },
        )

    def _extract_keywords(self, service_name: str, category_code: str = "") -> list[str]:
        keywords = []
        category_lower = category_code.lower() if category_code else ""
        service_name_lower = service_name.lower()

        if "lab" in category_lower or "xét nghiệm" in service_name_lower:
            keywords.extend(["cận lâm sàng", "xét nghiệm", "kiểm tra"])
        elif "img" in category_lower or any(token in service_name_lower for token in ["x-quang", "siêu âm", "ct", "mri"]):
            keywords.extend(["chẩn đoán hình ảnh", "x-quang", "siêu âm", "ct", "mri"])
        elif "end" in category_lower or "nội soi" in service_name_lower:
            keywords.extend(["nội soi", "thủ thuật"])
        elif "pat" in category_lower or "phẫu thuật" in service_name_lower:
            keywords.extend(["phẫu thuật", "giải phẫu"])
        elif "fun" in category_lower or "chức năng" in service_name_lower:
            keywords.extend(["thăm dò chức năng", "điện tâm đồ", "ecg"])
        elif "pro" in category_lower or "thủ thuật" in service_name_lower:
            keywords.extend(["thủ thuật", "khám"])

        words = [word.strip() for word in service_name.split() if len(word.strip()) > 2]
        keywords.extend(words[:3])
        return list(set(keywords))

    def _get_medical_contract_context(
        self,
        *,
        contract_id: str,
        service_code: str,
        disease_ids: list[str],
        sign_ids: list[str],
    ) -> dict[str, list[dict[str, Any]]]:
        params = {
            "contract_id": contract_id,
            "service_code": service_code,
            "disease_ids": disease_ids or [""],
            "sign_ids": sign_ids or [""],
            "namespace": INSURANCE_NAMESPACE,
        }
        disease_benefits = self._run_query(
            """
            MATCH (d:CIDisease)-[r:CI_SUPPORTS_CONTRACT_BENEFIT {contract_id:$contract_id}]->(b:Benefit {namespace:$namespace})
            WHERE d.disease_id IN $disease_ids
              AND coalesce(r.service_codes_json, '') CONTAINS $service_code
            OPTIONAL MATCH (b)-[g:GROUNDED_IN_CONTRACT_CLAUSE]->(cc:ContractClause {namespace:$namespace})
            RETURN d.disease_id AS disease_id,
                   d.disease_name AS disease_name,
                   b.entry_id AS benefit_entry_id,
                   b.entry_label AS benefit_label,
                   r.support_rows AS support_rows,
                   r.supporting_service_count AS supporting_service_count,
                   r.medical_role_distribution_json AS medical_role_distribution_json,
                   collect(DISTINCT {
                       clause_id: cc.clause_id,
                       clause_code: cc.clause_code,
                       clause_title: cc.clause_title,
                       match_score: g.match_score
                   })[0..3] AS clauses
            ORDER BY coalesce(r.support_rows, 0) DESC, disease_name
            LIMIT 12
            """,
            params,
        )
        for row in disease_benefits:
            row["clauses"] = [
                clause for clause in (row.get("clauses") or [])
                if clause and (clause.get("clause_id") or clause.get("clause_code"))
            ]
        disease_exclusions = self._run_query(
            """
            MATCH (d:CIDisease)-[r:CI_FLAGGED_BY_CONTRACT_EXCLUSION {contract_id:$contract_id}]->(e:Exclusion {namespace:$namespace})
            WHERE d.disease_id IN $disease_ids
              AND coalesce(r.service_codes_json, '') CONTAINS $service_code
            OPTIONAL MATCH (e)-[g:SUPPORTED_BY_RULEBOOK]->(rb:Rulebook {namespace:$namespace})
            RETURN d.disease_id AS disease_id,
                   d.disease_name AS disease_name,
                   e.code AS exclusion_code,
                   e.group AS exclusion_group,
                   e.reason AS exclusion_reason,
                   r.resolution_type AS resolution_type,
                   r.matched_rows AS matched_rows,
                   collect(DISTINCT {
                       rulebook_id: rb.rulebook_id,
                       rule_code: rb.rule_code,
                       rulebook_name: rb.display_name,
                       match_score: g.match_score
                   })[0..3] AS rulebooks
            ORDER BY coalesce(r.matched_rows, 0) DESC, disease_name
            LIMIT 12
            """,
            params,
        )
        sign_benefits = self._run_query(
            """
            MATCH (s:CISign)-[r:CI_SUPPORTS_CONTRACT_BENEFIT {contract_id:$contract_id}]->(b:Benefit {namespace:$namespace})
            WHERE s.sign_id IN $sign_ids
              AND coalesce(r.service_codes_json, '') CONTAINS $service_code
            RETURN s.sign_id AS sign_id,
                   coalesce(s.canonical_label, s.text) AS sign_label,
                   b.entry_id AS benefit_entry_id,
                   b.entry_label AS benefit_label,
                   r.support_rows AS support_rows,
                   r.supporting_disease_count AS supporting_disease_count
            ORDER BY coalesce(r.support_rows, 0) DESC, sign_label
            LIMIT 12
            """,
            params,
        )
        sign_exclusions = self._run_query(
            """
            MATCH (s:CISign)-[r:CI_FLAGGED_BY_CONTRACT_EXCLUSION {contract_id:$contract_id}]->(e:Exclusion {namespace:$namespace})
            WHERE s.sign_id IN $sign_ids
              AND coalesce(r.service_codes_json, '') CONTAINS $service_code
            RETURN s.sign_id AS sign_id,
                   coalesce(s.canonical_label, s.text) AS sign_label,
                   e.code AS exclusion_code,
                   e.group AS exclusion_group,
                   e.reason AS exclusion_reason,
                   r.resolution_type AS resolution_type,
                   r.matched_rows AS matched_rows
            ORDER BY coalesce(r.matched_rows, 0) DESC, sign_label
            LIMIT 12
            """,
            params,
        )
        contextual_support_strength: dict[str, int] = {}
        for row in disease_benefits + sign_benefits:
            benefit_id = str(row.get("benefit_entry_id") or "").strip()
            if not benefit_id:
                continue
            support_strength = int(row.get("support_rows") or 0)
            contextual_support_strength[benefit_id] = max(
                support_strength,
                contextual_support_strength.get(benefit_id, 0),
            )
        preferred_benefit_ids: set[str] = set()
        if contextual_support_strength:
            max_strength = max(contextual_support_strength.values())
            min_strength = max(5, int(max_strength * 0.25))
            preferred_benefit_ids = {
                benefit_id
                for benefit_id, strength in contextual_support_strength.items()
                if strength >= min_strength
            }
            disease_benefits = [
                row for row in disease_benefits
                if str(row.get("benefit_entry_id") or "") in preferred_benefit_ids
            ] or disease_benefits
            sign_benefits = [
                row for row in sign_benefits
                if str(row.get("benefit_entry_id") or "") in preferred_benefit_ids
            ] or sign_benefits
        policy_grounding = self._run_query(
            """
            MATCH (svc:CIService {service_code:$service_code})-[r:SUPPORTED_BY_CONTRACT_BENEFIT {contract_id:$contract_id}]->(b:Benefit {namespace:$namespace})
            OPTIONAL MATCH (b)-[g:GROUNDED_IN_CONTRACT_CLAUSE]->(cc:ContractClause {namespace:$namespace})
            RETURN b.entry_id AS benefit_entry_id,
                   b.entry_label AS benefit_label,
                   cc.clause_id AS clause_id,
                   cc.clause_code AS clause_code,
                   cc.clause_title AS clause_title,
                   g.match_score AS match_score
            ORDER BY coalesce(g.match_score, 0.0) DESC, clause_code
            LIMIT 10
            """,
            params,
        )
        policy_grounding = [
            row for row in policy_grounding
            if row.get("clause_id") or row.get("clause_code")
        ]
        if preferred_benefit_ids:
            filtered_grounding = [
                row for row in policy_grounding
                if str(row.get("benefit_entry_id") or "") in preferred_benefit_ids
            ]
            if filtered_grounding:
                policy_grounding = filtered_grounding
        exclusion_grounding = self._run_query(
            """
            MATCH (svc:CIService {service_code:$service_code})-[r:EXCLUDED_BY_CONTRACT {contract_id:$contract_id}]->(e:Exclusion {namespace:$namespace})
            OPTIONAL MATCH (e)-[g:SUPPORTED_BY_RULEBOOK]->(rb:Rulebook {namespace:$namespace})
            RETURN e.code AS exclusion_code,
                   e.group AS exclusion_group,
                   e.reason AS exclusion_reason,
                   rb.rulebook_id AS rulebook_id,
                   rb.rule_code AS rule_code,
                   rb.display_name AS rulebook_name,
                   g.match_score AS match_score
            ORDER BY coalesce(g.match_score, 0.0) DESC, exclusion_code
            LIMIT 10
            """,
            params,
        )
        return {
            "disease_benefits": disease_benefits,
            "disease_exclusions": disease_exclusions,
            "sign_benefits": sign_benefits,
            "sign_exclusions": sign_exclusions,
            "policy_grounding": policy_grounding,
            "exclusion_grounding": exclusion_grounding,
        }

    @staticmethod
    def _is_drug_like_service(category_code: str, service_name: str) -> bool:
        text = f"{category_code} {service_name}".lower()
        return any(token in text for token in ["thuốc", "drug", "pharmacy", "rx", "med"])

    @classmethod
    def _classify_exclusion_signal(
        cls,
        exclusion: dict[str, Any],
        category_code: str = "",
        service_name: str = "",
    ) -> str:
        code = (exclusion.get("code", "") or "").lower()
        group = (exclusion.get("group", "") or "").lower()
        reason = (
            exclusion.get("exclusion_reason_text")
            or exclusion.get("reason")
            or ""
        ).lower()
        source = (exclusion.get("match_source", "") or "").lower()
        text = f"{group} {reason}"

        financial_codes = {"ma01", "ma04", "ma11", "ma12", "ld0001", "ma003"}
        documentation_codes = {"ma07", "ma08", "ma09", "ma10", "ma19", "nan"}
        soft_review_codes = {"ma06", "ma39"}
        hard_deny_codes = {"ma05", "ma15", "ma21", "ma26", "ma27", "ma28", "ma30", "ma004"}
        drug_rule_codes = {"ma03", "ma16", "ma17", "ma18", "ma20", "m32", "m33", "ma31"}

        if code in financial_codes:
            return "financial"
        if code in documentation_codes:
            return "documentation"
        if code in drug_rule_codes:
            return "hard_deny" if cls._is_drug_like_service(category_code, service_name) else "ignore"
        if code in hard_deny_codes:
            return "hard_deny"
        if code in soft_review_codes:
            return "soft_review"

        if source == "keyword_contract_exclusion":
            return "soft_review"
        if any(token in text for token in ["đồng chi trả", "hạn mức", "vượt quá quyền lợi", "giảm trừ"]):
            return "financial"
        if any(token in text for token in ["chứng từ", "hóa đơn", "bồi thường"]):
            return "documentation"
        if any(token in text for token in ["tầm soát", "kiểm tra", "không có hướng điều trị"]):
            return "soft_review"
        if any(token in text for token in ["không thuộc quyền lợi", "loại trừ", "thời gian chờ", "blacklist"]):
            return "hard_deny"
        return "review"

    @staticmethod
    def _exclusion_priority(exclusion_type: str) -> int:
        order = {
            "hard_deny": 4,
            "documentation": 3,
            "financial": 2,
            "soft_review": 1,
            "review": 1,
            "ignore": 0,
        }
        return order.get(exclusion_type, 0)

    def _check_preauth_requirement(self, contract_id: str, service_cost_vnd: float) -> dict[str, Any]:
        contract = self._get_contract_info(contract_id)
        requires_preauth = contract.get("requires_preauth", False)
        if not requires_preauth:
            return {"required": False, "threshold": None}

        preauth_threshold = 500000
        return {
            "required": requires_preauth,
            "threshold": preauth_threshold,
            "exceeded": service_cost_vnd >= preauth_threshold,
            "service_cost": service_cost_vnd,
        }

    def _check_positive_result_requirement(self, contract_id: str, diagnosis_text: str = "") -> dict[str, Any]:
        contract = self._get_contract_info(contract_id)
        requires_positive = contract.get("positive_result_required", False)
        positive_keywords = ["dương tính", "âm tính", "xét nghiệm"]
        has_indicators = any(keyword in diagnosis_text.lower() for keyword in positive_keywords)
        return {
            "required": requires_positive,
            "has_indicators": has_indicators,
            "meets_requirement": not requires_positive or has_indicators,
        }

    def _get_exclusion_by_code(self, code: str) -> dict[str, Any] | None:
        result = self._run_query(
            """
            MATCH (e:Exclusion {code: $code})
            WHERE coalesce(e.namespace, '') = $namespace
            OPTIONAL MATCH (e)-[:HAS_REASON]->(er:ExclusionReason)
            RETURN e.code AS code,
                   e.group AS group,
                   e.reason AS reason,
                   er.text AS exclusion_reason_text,
                   e.usage_total_rows AS usage_total_rows,
                   e.usage_gap_vnd AS usage_gap_vnd
            LIMIT 1
            """,
            {"code": code, "namespace": INSURANCE_NAMESPACE},
        )
        return result[0] if result else None

    def assess(
        self,
        line: ServiceLineInput,
        service_info: dict[str, Any],
        medical_status: str = "uncertain",
    ) -> AgentVerdict:
        contract_id = line.contract_id or ""
        service_name_raw = line.service_name_raw or ""
        service_code = service_info.get("service_code", "")
        category_code = service_info.get("category_code", "")

        evidence: list[EvidenceItem] = []
        flags: list[str] = []
        decision: Decision = "uncertain"
        confidence: float = 0.5
        reasoning_vi: str = ""
        medical_context = self._resolve_medical_context(line, service_info)

        contract = self._get_contract_info(contract_id)
        if not contract:
            evidence.append(
                EvidenceItem(
                    source="contract_check",
                    key="contract_not_found",
                    value=f"Contract {contract_id} not found in database",
                    weight=1.0,
                )
            )
            return AgentVerdict(
                agent_name=self.agent_name,
                decision="deny",
                confidence=0.9,
                evidence=evidence,
                flags=["no_contract"],
                reasoning_vi=f"Không tìm thấy hợp đồng bảo hiểm: {contract_id}",
            )

        insurer = contract.get("insurer", "UNKNOWN")
        evidence.append(
            EvidenceItem(
                source="contract_info",
                key=contract_id,
                value=f"Contract: {contract.get('product_name', contract_id)} (Insurer: {insurer})",
                weight=0.5,
            )
        )

        disease_ids = [item.get("disease_id", "") for item in medical_context.get("diseases", []) if item.get("disease_id")]
        sign_ids = [item.get("sign_id", "") for item in medical_context.get("signs", []) if item.get("sign_id")]
        contextual_graph = {
            "disease_benefits": [],
            "disease_exclusions": [],
            "sign_benefits": [],
            "sign_exclusions": [],
            "policy_grounding": [],
            "exclusion_grounding": [],
        }
        if service_code and contract_id and (disease_ids or sign_ids):
            contextual_graph = self._get_medical_contract_context(
                contract_id=contract_id,
                service_code=service_code,
                disease_ids=disease_ids,
                sign_ids=sign_ids,
            )

        disease_benefits = contextual_graph.get("disease_benefits") or []
        disease_exclusions = contextual_graph.get("disease_exclusions") or []
        sign_benefits = contextual_graph.get("sign_benefits") or []
        sign_exclusions = contextual_graph.get("sign_exclusions") or []

        if disease_benefits:
            top = disease_benefits[0]
            evidence.append(
                EvidenceItem(
                    source="medical_contract_context",
                    key=top.get("disease_id", ""),
                    value=(
                        f"Benh '{top.get('disease_name', '')}' co duong dan hop dong toi "
                        f"quyen loi '{top.get('benefit_label', '')}' cho dich vu nay"
                    ),
                    weight=0.75,
                )
            )
            flags.append("disease_contract_support")
        if sign_benefits:
            top = sign_benefits[0]
            evidence.append(
                EvidenceItem(
                    source="medical_contract_context",
                    key=top.get("sign_id", ""),
                    value=(
                        f"Dau hieu '{top.get('sign_label', '')}' co lien ket den quy trinh "
                        f"chi tra cho dich vu nay trong hop dong"
                    ),
                    weight=0.65,
                )
            )
            flags.append("sign_contract_support")
        if disease_exclusions:
            top = disease_exclusions[0]
            evidence.append(
                EvidenceItem(
                    source="medical_contract_context",
                    key=top.get("exclusion_code", ""),
                    value=(
                        f"Benh '{top.get('disease_name', '')}' dang cham exclusion "
                        f"{top.get('exclusion_code', '')}: {top.get('exclusion_reason', '')}"
                    ),
                    weight=0.72,
                )
            )
            flags.append("disease_contract_exclusion")
        if sign_exclusions:
            top = sign_exclusions[0]
            evidence.append(
                EvidenceItem(
                    source="medical_contract_context",
                    key=top.get("exclusion_code", ""),
                    value=(
                        f"Dau hieu '{top.get('sign_label', '')}' dan toi exclusion "
                        f"{top.get('exclusion_code', '')}: {top.get('exclusion_reason', '')}"
                    ),
                    weight=0.62,
                )
            )
            flags.append("sign_contract_exclusion")

        benefits = self._get_benefits_for_service(
            contract_id,
            service_name_raw,
            service_code=service_code,
            category_code=category_code,
        )
        if benefits:
            evidence.append(
                EvidenceItem(
                    source="benefit_match",
                    key=f"found_{len(benefits)}_benefits",
                    value=f"Found {len(benefits)} potential benefit(s): {', '.join(item.get('entry_label', '') for item in benefits[:3])}",
                    weight=0.7,
                )
            )
            flags.append("benefit_match_found")
        else:
            evidence.append(
                EvidenceItem(
                    source="benefit_match",
                    key="no_benefit_match",
                    value=f"No benefit found matching service: {service_name_raw}",
                    weight=0.9,
                )
            )
            flags.append("no_benefit_match")

        if not benefits and (disease_benefits or sign_benefits):
            flags.append("contextual_benefit_support")

        exclusions = self._search_exclusions(
            contract_id,
            service_name_raw,
            service_code=service_code,
            category_code=category_code,
        )
        filtered_exclusions: list[dict[str, Any]] = []
        for exclusion in exclusions:
            exclusion_type = self._classify_exclusion_signal(
                exclusion,
                category_code=category_code,
                service_name=service_name_raw,
            )
            if exclusion_type == "ignore":
                continue
            enriched = dict(exclusion)
            enriched["resolution_type"] = exclusion_type
            filtered_exclusions.append(enriched)

        if filtered_exclusions:
            filtered_exclusions.sort(
                key=lambda item: (
                    self._exclusion_priority(item.get("resolution_type", "")),
                    int(item.get("matched_rows", 0) or 0),
                    int(item.get("usage_total_rows", 0) or 0),
                ),
                reverse=True,
            )
            top_exclusion = filtered_exclusions[0]
            evidence.append(
                EvidenceItem(
                    source="exclusion_check",
                    key=top_exclusion.get("code", ""),
                    value=f"Exclusion found: {top_exclusion.get('exclusion_reason_text') or top_exclusion.get('reason', '')}",
                    weight=0.95,
                )
            )
            flags.append("exclusion_flagged")
            if top_exclusion.get("resolution_type") == "financial":
                flags.append("financial_adjustment")
            reasoning_vi = f"Dịch vụ có thể thuộc loại trừ: {top_exclusion.get('exclusion_reason_text') or top_exclusion.get('reason', '')}"
        else:
            evidence.append(
                EvidenceItem(
                    source="exclusion_check",
                    key="no_exclusion_match",
                    value=f"No exclusion found for service: {service_name_raw}",
                    weight=0.5,
                )
            )

        exclusion_types = {item.get("resolution_type", "") for item in filtered_exclusions}
        if "hard_deny" in exclusion_types:
            top_exclusion = filtered_exclusions[0]
            exclusion_usage = int(top_exclusion.get("usage_total_rows", 0) or 0)
            decision = "deny"
            confidence = 0.8
            if exclusion_usage > 0:
                reasoning_vi = f"Dịch vụ thuộc loại trừ thường gặp (đã xuất hiện {exclusion_usage} hồ sơ)"
            else:
                reasoning_vi = "Dịch vụ có bằng chứng loại trừ trực tiếp theo hợp đồng/quy tắc"
        elif "documentation" in exclusion_types:
            decision = "review"
            confidence = 0.65
            reasoning_vi = "Hồ sơ có tín hiệu thiếu/chưa hợp lệ về chứng từ, cần kiểm tra thủ công trước khi chốt chi trả"
        elif ("financial" in exclusion_types or "financial_adjustment" in flags) and benefits:
            decision = "partial_pay"
            confidence = 0.8
            reasoning_vi = "Dịch vụ thuộc quyền lợi nhưng bị điều chỉnh theo quy tắc tài chính/đồng chi trả, cần tính mức chi trả thay vì từ chối toàn bộ"
        elif "soft_review" in exclusion_types or "review" in exclusion_types:
            decision = "review"
            confidence = 0.55
            reasoning_vi = "Có tín hiệu loại trừ ở mức nhóm quy tắc/hợp đồng, nhưng chưa có bằng chứng dịch vụ-level đủ mạnh để từ chối tự động"
        elif "no_benefit_match" in flags:
            decision = "deny"
            confidence = 0.85
            reasoning_vi = f"Dịch vụ không thuộc quyền lợi bảo hiểm theo hợp đồng {contract_id}"
        elif benefits and medical_status == "approve":
            decision = "approve"
            confidence = 0.8
            reasoning_vi = "Dịch vụ thuộc quyền lợi và được xác nhận hợp lý về mặt y khoa"
        elif benefits and medical_status in {"review", "uncertain"}:
            decision = "review"
            confidence = 0.6
            reasoning_vi = "Dịch vụ thuộc quyền lợi nhưng cần xem xét thêm về tính hợp lý y khoa"
        elif benefits and medical_status == "deny":
            decision = "deny"
            confidence = 0.75
            reasoning_vi = "Dịch vụ thuộc quyền lợi nhưng không hợp lý về mặt y khoa"
        else:
            decision = "review"
            confidence = 0.5
            reasoning_vi = "Cần xem xét thủ công do thiếu thông tin"

        contextual_support_found = bool(disease_benefits or sign_benefits)
        contextual_exclusion_found = bool(disease_exclusions or sign_exclusions)
        if decision == "deny" and "no_benefit_match" in flags and contextual_support_found:
            decision = "review"
            confidence = max(confidence, 0.58)
            reasoning_vi = (
                "Chưa thấy direct benefit match ở service-level, nhưng graph đã có đường bệnh/dấu hiệu → quyền lợi "
                "cho dịch vụ này nên cần review thủ công thay vì từ chối ngay."
            )
        elif decision in {"review", "uncertain"} and contextual_support_found and benefits and medical_status == "supported_by_final_diagnosis" and "hard_deny" not in exclusion_types:
            if "financial" in exclusion_types:
                decision = "partial_pay"
                confidence = max(confidence, 0.78)
                reasoning_vi = (
                    "Dịch vụ có support từ bệnh/dấu hiệu và quyền lợi hợp đồng, nhưng đồng thời có rule tài chính/đồng chi trả; "
                    "nên chuyển sang partial pay thay vì review thuần."
                )
            else:
                decision = "approve"
                confidence = max(confidence, 0.76)
                reasoning_vi = (
                    "Dịch vụ có support y khoa, có đường bệnh/dấu hiệu → quyền lợi trong graph, "
                    "và không thấy hard exclusion đủ mạnh ở contract-level."
                )
        elif decision == "deny" and contextual_support_found and exclusion_types and exclusion_types.issubset({"soft_review", "documentation"}):
            decision = "review"
            confidence = max(confidence, 0.6)
            reasoning_vi = (
                "Có support từ bệnh/dấu hiệu, nhưng các exclusion hiện tại mới ở mức soft/documentation; "
                "cần review thủ công thay vì hard deny."
            )
        elif decision == "deny" and contextual_exclusion_found and "hard_deny" in exclusion_types:
            confidence = max(confidence, 0.88)

        if contextual_graph.get("policy_grounding"):
            top_clause = contextual_graph["policy_grounding"][0]
            clause_key = top_clause.get("clause_code") or top_clause.get("clause_id") or ""
            if clause_key:
                evidence.append(
                    EvidenceItem(
                        source="policy_grounding",
                        key=clause_key,
                        value=f"Căn cứ clause: {top_clause.get('clause_title', '')}",
                        weight=0.7,
                    )
                )
        if contextual_graph.get("exclusion_grounding"):
            top_rulebook = contextual_graph["exclusion_grounding"][0]
            if top_rulebook.get("rulebook_id"):
                evidence.append(
                    EvidenceItem(
                        source="policy_grounding",
                        key=top_rulebook.get("rule_code") or top_rulebook.get("rulebook_id", ""),
                        value=f"Căn cứ rulebook: {top_rulebook.get('rulebook_name', '')}",
                        weight=0.7,
                    )
                )

        if service_code:
            try:
                clinical_info = self._run_query(
                    """
                    MATCH (s:CIService {service_code: $code})
                    OPTIONAL MATCH (d:CIDisease)-[:CI_INDICATES_SERVICE]->(s)
                    RETURN s.service_name AS service_name,
                           s.category_name AS category,
                           collect(DISTINCT d.disease_name) AS related_diseases
                    LIMIT 1
                    """,
                    {"code": service_code},
                )
                if clinical_info:
                    info = clinical_info[0]
                    evidence.append(
                        EvidenceItem(
                            source="clinical_data",
                            key=service_code,
                            value=f"Clinical data: {info.get('service_name', '')} ({info.get('category', '')}) - Related diseases: {', '.join(info.get('related_diseases', [])[:3])}",
                            weight=0.6,
                        )
                    )
            except Exception as exc:
                logger.debug("Could not fetch clinical info for %s: %s", service_code, exc)

        return AgentVerdict(
            agent_name=self.agent_name,
            decision=decision,
            confidence=confidence,
            evidence=evidence,
            flags=flags,
            reasoning_vi=reasoning_vi,
            meta={
                "contract_id": contract_id,
                "insurer": insurer,
                "service_code": service_code,
                "category_code": category_code,
                "benefits_found": len(benefits),
                "exclusions_found": len(filtered_exclusions),
                "medical_status": medical_status,
                "medical_context": medical_context,
                "contextual_graph": contextual_graph,
            },
        )


_neo4j_agent_instance: ContractAgentNeo4j | None = None


def get_neo4j_contract_agent() -> ContractAgentNeo4j:
    global _neo4j_agent_instance
    if _neo4j_agent_instance is None:
        _neo4j_agent_instance = ContractAgentNeo4j()
    return _neo4j_agent_instance


def close_neo4j_contract_agent():
    global _neo4j_agent_instance
    if _neo4j_agent_instance is not None:
        _neo4j_agent_instance.close()
        _neo4j_agent_instance = None
