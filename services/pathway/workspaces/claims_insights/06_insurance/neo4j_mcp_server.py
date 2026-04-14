"""Neo4j MCP Server for Pathway Adjudication Agents.

This MCP server provides tools for agents to query insurance knowledge,
clinical data, and contract rules from Neo4j.

Usage:
    python neo4j_mcp_server.py

The server will listen on stdio for MCP protocol messages.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from neo4j import GraphDatabase

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Neo4j connection
NEO4J_URI = "bolt://localhost:7688"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password123"


# Create MCP server
server = Server("pathway-neo4j")


class Neo4jKnowledgeStore:
    """Neo4j knowledge store for adjudication queries."""

    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """Close Neo4j driver."""
        if self.driver:
            self.driver.close()

    def query_contracts_by_insurer(self, insurer: str) -> list[dict]:
        """Query all contracts for a given insurer."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (i:Insurer {name: $insurer})-[:ISSUES]->(c:InsuranceContract)
                RETURN c.contract_id AS contract_id,
                       c.product_name AS product_name,
                       c.mode AS mode,
                       c.requires_preauth AS requires_preauth,
                       c.positive_result_required AS positive_result_required,
                       c.paid_ratio_pct AS paid_ratio_pct
                ORDER BY c.contract_id
            """, insurer=insurer)
            return [dict(record) for record in result]

    def query_benefits_for_contract(self, contract_id: str, benefit_name: str = "") -> list[dict]:
        """Query benefits for a specific contract."""
        with self.driver.session() as session:
            if benefit_name:
                result = session.run("""
                    MATCH (c:InsuranceContract {contract_id: $contract_id})-[:HAS_BENEFIT]->(b:Benefit)
                    WHERE toLower(b.canonical_name) CONTAINS toLower($benefit_name)
                    RETURN b.entry_id AS entry_id,
                           b.entry_label AS entry_label,
                           b.major_section AS major_section,
                           b.subsection AS subsection,
                           b.canonical_name AS canonical_name
                    ORDER BY b.entry_label
                    LIMIT 20
                """, contract_id=contract_id, benefit_name=benefit_name)
            else:
                result = session.run("""
                    MATCH (c:InsuranceContract {contract_id: $contract_id})-[:HAS_BENEFIT]->(b:Benefit)
                    RETURN b.entry_id AS entry_id,
                           b.entry_label AS entry_label,
                           b.major_section AS major_section,
                           b.subsection AS subsection,
                           b.canonical_name AS canonical_name
                    ORDER BY b.entry_label
                    LIMIT 50
                """, contract_id=contract_id)
            return [dict(record) for record in result]

    def query_exclusions_by_contract(self, contract_id: str) -> list[dict]:
        """Query exclusions associated with a contract."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c:InsuranceContract {contract_id: $contract_id})-[:HAS_EXCLUSION]->(e:Exclusion)
                OPTIONAL MATCH (e)-[:HAS_REASON]->(er:ExclusionReason)
                RETURN e.code AS code,
                       e.group AS group,
                       e.reason AS reason,
                       e.process_path AS process_path,
                       e.source_note AS source_note,
                       er.text AS exclusion_reason_text,
                       e.usage_total_rows AS usage_total_rows,
                       e.usage_gap_vnd AS usage_gap_vnd
                ORDER BY e.usage_total_rows DESC
                LIMIT 50
            """, contract_id=contract_id)
            return [dict(record) for record in result]

    def query_exclusion_by_reason_text(self, reason_text: str) -> list[dict]:
        """Search exclusions by reason text."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (e:Exclusion)-[:HAS_REASON]->(er:ExclusionReason)
                WHERE toLower(er.text) CONTAINS toLower($reason_text)
                   OR toLower(e.reason) CONTAINS toLower($reason_text)
                RETURN DISTINCT e.code AS code,
                       e.group AS group,
                       e.reason AS reason,
                       er.text AS exclusion_reason_text,
                       e.usage_total_rows AS usage_total_rows,
                       e.usage_gap_vnd AS usage_gap_vnd
                ORDER BY e.usage_total_rows DESC
                LIMIT 20
            """, reason_text=reason_text)
            return [dict(record) for record in result]

    def query_rulebook_by_insurer(self, insurer: str) -> list[dict]:
        """Query rulebooks for an insurer."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (i:Insurer {name: $insurer})-[:HAS_RULEBOOK]->(r:Rulebook)
                RETURN r.rulebook_id AS rulebook_id,
                       r.rule_code AS rule_code,
                       r.display_name AS display_name,
                       r.page_count AS page_count,
                       r.ocr_status AS ocr_status,
                       r.total_claim_evidence_rows AS claim_evidence_rows
                ORDER BY r.rule_code
            """, insurer=insurer)
            return [dict(record) for record in result]

    def query_plans_for_contract(self, contract_id: str) -> list[dict]:
        """Query available plans for a contract."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c:InsuranceContract {contract_id: $contract_id})-[:COVERS_PLAN]->(p:ContractPlan)
                RETURN p.plan_id AS plan_id,
                       p.name AS name,
                       p.contract_id AS contract_id
                ORDER BY p.name
            """, contract_id=contract_id)
            return [dict(record) for record in result]

    def query_plan_coverage_for_benefit(self, contract_id: str, plan_name: str, benefit_label: str) -> list[dict]:
        """Check if a benefit is covered under a specific plan."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c:InsuranceContract {contract_id: $contract_id})
                MATCH (c)-[:COVERS_PLAN]->(p:ContractPlan {name: $plan_name})
                MATCH (p)-[:COVERS_BENEFIT]->(b:Benefit)
                WHERE b.contract_id = $contract_id
                  AND toLower(b.entry_label) = toLower($benefit_label)
                RETURN b.entry_label AS benefit_label,
                       p.name AS plan_name,
                       r.coverage AS coverage
                LIMIT 1
            """, contract_id=contract_id, plan_name=plan_name, benefit_label=benefit_label)
            return [dict(record) for record in result]

    def query_service_exclusions(self, service_code: str = "", service_name: str = "") -> list[dict]:
        """Query exclusions related to a service (code or name)."""
        with self.driver.session() as session:
            if service_code:
                result = session.run("""
                    MATCH (s:Service {code: $code})-[:EXCLUDED_BY]->(e:Exclusion)
                    OPTIONAL MATCH (e)-[:HAS_REASON]->(er:ExclusionReason)
                    RETURN s.code AS service_code,
                           s.canonical_name AS service_name,
                           e.code AS exclusion_code,
                           e.group AS exclusion_group,
                           e.reason AS exclusion_reason,
                           er.text AS exclusion_reason_text
                    LIMIT 20
                """, code=service_code)
            elif service_name:
                result = session.run("""
                    MATCH (s:Service)-[:EXCLUDED_BY]->(e:Exclusion)
                    WHERE toLower(s.canonical_name) CONTAINS toLower($name)
                    OPTIONAL MATCH (e)-[:HAS_REASON]->(er:ExclusionReason)
                    RETURN s.code AS service_code,
                           s.canonical_name AS service_name,
                           e.code AS exclusion_code,
                           e.group AS exclusion_group,
                           e.reason AS exclusion_reason,
                           er.text AS exclusion_reason_text
                    LIMIT 20
                """, name=service_name)
            else:
                return []
            return [dict(record) for record in result]

    def query_contract_stats(self, contract_id: str) -> dict:
        """Get statistical summary for a contract."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c:InsuranceContract {contract_id: $contract_id})
                OPTIONAL MATCH (c)-[:HAS_BENEFIT]->(b:Benefit)
                OPTIONAL MATCH (c)-[:HAS_EXCLUSION]->(e:Exclusion)
                OPTIONAL MATCH (c)-[:COVERS_PLAN]->(p:ContractPlan)
                RETURN c.contract_id AS contract_id,
                       c.product_name AS product_name,
                       c.mode AS mode,
                       c.paid_ratio_pct AS paid_ratio_pct,
                       count(DISTINCT b) AS benefit_count,
                       count(DISTINCT e) AS exclusion_count,
                       count(DISTINCT p) AS plan_count
                LIMIT 1
            """, contract_id=contract_id)
            records = [dict(record) for record in result]
            return records[0] if records else {}

    def query_clinical_service_info(self, service_code: str) -> dict:
        """Query clinical information about a service from Neo4j."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (s:Service {code: $code})
                OPTIONAL MATCH (s)-[:MENTIONS]->(d:Disease)
                OPTIONAL MATCH (s)-[:MENTIONS]->(lt:LabTest)
                OPTIONAL MATCH (s)-[:MENTIONS]->(proc:Procedure)
                OPTIONAL MATCH (s)-[:ABOUT_DISEASE]->(d2:Disease)
                RETURN s.code AS code,
                       s.canonical_name AS canonical_name,
                       s.category_code AS category_code,
                       s.category_name AS category_name,
                       collect(DISTINCT d.icd_code) AS related_icds,
                       collect(DISTINCT d.name) AS related_diseases,
                       collect(DISTINCT lt.name) AS related_lab_tests,
                       collect(DISTINCT proc.name) AS related_procedures
                LIMIT 1
            """, code=service_code)
            records = [dict(record) for record in result]
            return records[0] if records else {}

    def query_disease_services(self, icd_code: str = "", disease_name: str = "") -> list[dict]:
        """Query services commonly used for a disease."""
        with self.driver.session() as session:
            if icd_code:
                result = session.run("""
                    MATCH (d:Disease {icd_code: $code})<-[:ABOUT_DISEASE]-(ch:Chunk)
                    OPTIONAL MATCH (ch)-[:MENTIONS]->(s:Service)
                    RETURN d.icd_code AS icd_code,
                           d.name AS disease_name,
                           collect(DISTINCT s.code) AS service_codes,
                           collect(DISTINCT s.canonical_name) AS service_names
                    LIMIT 1
                """, code=icd_code)
            elif disease_name:
                result = session.run("""
                    MATCH (d:Disease)
                    WHERE toLower(d.name) CONTAINS toLower($name)
                    MATCH (d)<-[:ABOUT_DISEASE]-(ch:Chunk)
                    OPTIONAL MATCH (ch)-[:MENTIONS]->(s:Service)
                    RETURN d.icd_code AS icd_code,
                           d.name AS disease_name,
                           collect(DISTINCT s.code) AS service_codes,
                           collect(DISTINCT s.canonical_name) AS service_names
                    LIMIT 5
                """, name=disease_name)
            else:
                return []
            return [dict(record) for record in result]


# Initialize knowledge store
store = Neo4jKnowledgeStore(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)


# ---------------------------------------------------------------------------
# MCP Tool Definitions
# ---------------------------------------------------------------------------

@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available MCP tools."""
    return [
        Tool(
            name="query_contracts_by_insurer",
            description="Query all contracts for a given insurer name (e.g., 'FPT', 'PJICO', 'BHV')",
            inputSchema={
                "type": "object",
                "properties": {
                    "insurer": {
                        "type": "string",
                        "description": "Insurer name (e.g., FPT, PJICO, BHV, TCGIns, UIC)"
                    }
                },
                "required": ["insurer"]
            }
        ),
        Tool(
            name="query_benefits_for_contract",
            description="Query benefits for a specific contract. Optionally filter by benefit name.",
            inputSchema={
                "type": "object",
                "properties": {
                    "contract_id": {
                        "type": "string",
                        "description": "Contract ID (e.g., FPT-NT-2024)"
                    },
                    "benefit_name": {
                        "type": "string",
                        "description": "Optional benefit name to filter (e.g., 'Điều trị ngoại trú')"
                    }
                },
                "required": ["contract_id"]
            }
        ),
        Tool(
            name="query_exclusions_by_contract",
            description="Query exclusions associated with a contract, sorted by usage frequency.",
            inputSchema={
                "type": "object",
                "properties": {
                    "contract_id": {
                        "type": "string",
                        "description": "Contract ID"
                    }
                },
                "required": ["contract_id"]
            }
        ),
        Tool(
            name="query_exclusion_by_reason_text",
            description="Search exclusions by reason text (e.g., 'thuốc', 'cận lâm sàng').",
            inputSchema={
                "type": "object",
                "properties": {
                    "reason_text": {
                        "type": "string",
                        "description": "Reason text to search for in exclusions"
                    }
                },
                "required": ["reason_text"]
            }
        ),
        Tool(
            name="query_rulebook_by_insurer",
            description="Query rulebooks for an insurer (QT 384, QT 711, etc.).",
            inputSchema={
                "type": "object",
                "properties": {
                    "insurer": {
                        "type": "string",
                        "description": "Insurer name"
                    }
                },
                "required": ["insurer"]
            }
        ),
        Tool(
            name="query_plans_for_contract",
            description="Query available plans for a contract.",
            inputSchema={
                "type": "object",
                "properties": {
                    "contract_id": {
                        "type": "string",
                        "description": "Contract ID"
                    }
                },
                "required": ["contract_id"]
            }
        ),
        Tool(
            name="query_plan_coverage_for_benefit",
            description="Check if a specific benefit is covered under a plan.",
            inputSchema={
                "type": "object",
                "properties": {
                    "contract_id": {
                        "type": "string",
                        "description": "Contract ID"
                    },
                    "plan_name": {
                        "type": "string",
                        "description": "Plan name (e.g., Gói A, Gói B)"
                    },
                    "benefit_label": {
                        "type": "string",
                        "description": "Benefit label to check"
                    }
                },
                "required": ["contract_id", "plan_name", "benefit_label"]
            }
        ),
        Tool(
            name="query_service_exclusions",
            description="Query exclusions related to a service by code or name.",
            inputSchema={
                "type": "object",
                "properties": {
                    "service_code": {
                        "type": "string",
                        "description": "Service code (e.g., LAB-BIO-002)"
                    },
                    "service_name": {
                        "type": "string",
                        "description": "Service name (used if code not provided)"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="query_contract_stats",
            description="Get statistical summary for a contract (benefits, exclusions, plans counts).",
            inputSchema={
                "type": "object",
                "properties": {
                    "contract_id": {
                        "type": "string",
                        "description": "Contract ID"
                    }
                },
                "required": ["contract_id"]
            }
        ),
        Tool(
            name="query_clinical_service_info",
            description="Query clinical information about a service from Neo4j (related diseases, lab tests, procedures).",
            inputSchema={
                "type": "object",
                "properties": {
                    "service_code": {
                        "type": "string",
                        "description": "Service code"
                    }
                },
                "required": ["service_code"]
            }
        ),
        Tool(
            name="query_disease_services",
            description="Query services commonly used for a disease (by ICD code or name).",
            inputSchema={
                "type": "object",
                "properties": {
                    "icd_code": {
                        "type": "string",
                        "description": "ICD-10 code (e.g., J18.9)"
                    },
                    "disease_name": {
                        "type": "string",
                        "description": "Disease name (used if ICD code not provided)"
                    }
                },
                "required": []
            }
        ),
    ]


# ---------------------------------------------------------------------------
# MCP Tool Handlers
# ---------------------------------------------------------------------------

@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """Handle MCP tool calls."""
    logger.info(f"Tool call: {name} with args: {arguments}")

    try:
        result = []

        if name == "query_contracts_by_insurer":
            insurer = arguments.get("insurer")
            records = store.query_contracts_by_insurer(insurer)
            result = [TextContent(type="text", text=json.dumps(records, ensure_ascii=False, indent=2))]

        elif name == "query_benefits_for_contract":
            contract_id = arguments.get("contract_id")
            benefit_name = arguments.get("benefit_name", "")
            records = store.query_benefits_for_contract(contract_id, benefit_name)
            result = [TextContent(type="text", text=json.dumps(records, ensure_ascii=False, indent=2))]

        elif name == "query_exclusions_by_contract":
            contract_id = arguments.get("contract_id")
            records = store.query_exclusions_by_contract(contract_id)
            result = [TextContent(type="text", text=json.dumps(records, ensure_ascii=False, indent=2))]

        elif name == "query_exclusion_by_reason_text":
            reason_text = arguments.get("reason_text")
            records = store.query_exclusion_by_reason_text(reason_text)
            result = [TextContent(type="text", text=json.dumps(records, ensure_ascii=False, indent=2))]

        elif name == "query_rulebook_by_insurer":
            insurer = arguments.get("insurer")
            records = store.query_rulebook_by_insurer(insurer)
            result = [TextContent(type="text", text=json.dumps(records, ensure_ascii=False, indent=2))]

        elif name == "query_plans_for_contract":
            contract_id = arguments.get("contract_id")
            records = store.query_plans_for_contract(contract_id)
            result = [TextContent(type="text", text=json.dumps(records, ensure_ascii=False, indent=2))]

        elif name == "query_plan_coverage_for_benefit":
            contract_id = arguments.get("contract_id")
            plan_name = arguments.get("plan_name")
            benefit_label = arguments.get("benefit_label")
            records = store.query_plan_coverage_for_benefit(contract_id, plan_name, benefit_label)
            result = [TextContent(type="text", text=json.dumps(records, ensure_ascii=False, indent=2))]

        elif name == "query_service_exclusions":
            service_code = arguments.get("service_code", "")
            service_name = arguments.get("service_name", "")
            records = store.query_service_exclusions(service_code, service_name)
            result = [TextContent(type="text", text=json.dumps(records, ensure_ascii=False, indent=2))]

        elif name == "query_contract_stats":
            contract_id = arguments.get("contract_id")
            record = store.query_contract_stats(contract_id)
            result = [TextContent(type="text", text=json.dumps(record, ensure_ascii=False, indent=2))]

        elif name == "query_clinical_service_info":
            service_code = arguments.get("service_code")
            record = store.query_clinical_service_info(service_code)
            result = [TextContent(type="text", text=json.dumps(record, ensure_ascii=False, indent=2))]

        elif name == "query_disease_services":
            icd_code = arguments.get("icd_code", "")
            disease_name = arguments.get("disease_name", "")
            records = store.query_disease_services(icd_code, disease_name)
            result = [TextContent(type="text", text=json.dumps(records, ensure_ascii=False, indent=2))]

        else:
            result = [TextContent(type="text", text=f"Unknown tool: {name}")]

        return result

    except Exception as e:
        logger.error(f"Error in tool call: {e}", exc_info=True)
        return [TextContent(type="text", text=json.dumps({"error": str(e)}))]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    """Run the MCP server."""
    logger.info("Starting Pathway Neo4j MCP Server...")
    logger.info(f"Connecting to Neo4j at {NEO4J_URI}")

    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
