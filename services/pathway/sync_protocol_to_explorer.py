"""
Sync protocol knowledge (Disease→Drug/Symptom/LabTest/Procedure) from medical KG
into Claims Insights Disease Explorer format.

Reads entities extracted during PDF→Neo4j ingestion and transforms them into
the explorer's CIDisease/CISign/CIService/CIObservation schema.

Usage:
    cd notebooklm
    python sync_protocol_to_explorer.py              # sync all protocol diseases
    python sync_protocol_to_explorer.py --icd J00    # sync single ICD
    python sync_protocol_to_explorer.py --dry-run    # show what would change
"""

import json
import os
import sys
import io
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

from neo4j import GraphDatabase
from runtime_env import load_notebooklm_env

load_notebooklm_env()

BUNDLE_PATH = Path(__file__).parent / "static" / "claims_insights" / "disease_graph_explorer_data.json"


def _slugify(text: str) -> str:
    text = unicodedata.normalize("NFD", text)
    text = re.sub(r'[\u0300-\u036f]', '', text)  # strip diacritics
    text = re.sub(r'[^a-zA-Z0-9\s-]', '', text)
    text = re.sub(r'[\s]+', '-', text.strip()).lower()
    return text


def get_driver():
    uri = os.getenv("NEO4J_URI", os.getenv("neo4j_uri", "bolt://localhost:7688"))
    user = os.getenv("NEO4J_USER", os.getenv("neo4j_user", "neo4j"))
    pw = os.getenv("NEO4J_PASSWORD", os.getenv("neo4j_password", "password123"))
    return GraphDatabase.driver(uri, auth=(user, pw))


ICD_LOOKUP = {
    "Viêm mũi họng cấp": "J00",
    "Viêm phế quản cấp": "J20",
    "Viêm họng cấp": "J02",
    "Viêm mũi xoang cấp tính": "J01",
    "Viêm amidan cấp": "J03",
    "Nhiễm trùng đường hô hấp trên cấp tính": "J06",
    "Viêm mũi dị ứng": "J30",
    "Viêm phổi": "J18",
    "Viêm thanh quản và khí quản cấp": "J04",
    "Hen phế quản": "J45",
    "Sốt xuất huyết Dengue": "A97",
    "Viêm gan vi rút B": "B18.1",
}


def query_protocol_diseases(driver) -> list[dict]:
    """Get all diseases that have Chunk data (from protocol ingestion)."""
    with driver.session() as s:
        result = s.run("""
            MATCH (d:Disease)<-[:ABOUT_DISEASE]-(c:Chunk)
            WITH d, count(c) AS chunk_count
            RETURN d.name AS disease_name,
                   coalesce(d.icd_code, d.code, '') AS icd_code,
                   chunk_count
            ORDER BY chunk_count DESC
        """)
        diseases = []
        seen = set()
        for r in result:
            rec = dict(r)
            name = rec["disease_name"]
            if name in seen:
                continue
            seen.add(name)
            # Resolve ICD from lookup if missing
            if not rec["icd_code"]:
                rec["icd_code"] = ICD_LOOKUP.get(name, "")
            if not rec["icd_code"]:
                print(f"  [SKIP] {name} — no ICD code (add to ICD_LOOKUP)")
                continue
            diseases.append(rec)
        return diseases


def query_disease_entities(driver, disease_name: str) -> dict:
    """Query all entities linked to a disease via Chunk→MENTIONS."""
    with driver.session() as s:
        # Symptoms → Signs
        symptoms = s.run("""
            MATCH (d:Disease {name: $name})<-[:ABOUT_DISEASE]-(c:Chunk)-[:MENTIONS]->(e)
            WHERE e:Symptom OR (e:Entity AND e.type = 'Symptom')
            RETURN DISTINCT e.name AS name, count(c) AS support_chunks
            ORDER BY support_chunks DESC
        """, name=disease_name).data()

        # Drugs → Services (category: Thuốc)
        drugs = s.run("""
            MATCH (d:Disease {name: $name})<-[:ABOUT_DISEASE]-(c:Chunk)-[:MENTIONS]->(e)
            WHERE e:Drug OR (e:Entity AND e.type = 'Drug')
            RETURN DISTINCT e.name AS name, count(c) AS support_chunks
            ORDER BY support_chunks DESC
        """, name=disease_name).data()

        # Procedures → Services (category: Thủ thuật)
        procedures = s.run("""
            MATCH (d:Disease {name: $name})<-[:ABOUT_DISEASE]-(c:Chunk)-[:MENTIONS]->(e)
            WHERE e:Procedure OR (e:Entity AND e.type = 'Procedure')
            RETURN DISTINCT e.name AS name, count(c) AS support_chunks
            ORDER BY support_chunks DESC
        """, name=disease_name).data()

        # LabTests → Observations
        labs = s.run("""
            MATCH (d:Disease {name: $name})<-[:ABOUT_DISEASE]-(c:Chunk)-[:MENTIONS]->(e)
            WHERE e:LabTest OR (e:Entity AND e.type = 'LabTest')
            RETURN DISTINCT e.name AS name, count(c) AS support_chunks
            ORDER BY support_chunks DESC
        """, name=disease_name).data()

        # Complications, RiskFactors → extra signs
        extras = s.run("""
            MATCH (d:Disease {name: $name})<-[:ABOUT_DISEASE]-(c:Chunk)-[:MENTIONS]->(e)
            WHERE e:Complication OR e:RiskFactor
                  OR (e:Entity AND e.type IN ['Complication', 'RiskFactor'])
            RETURN DISTINCT e.name AS name, labels(e)[0] AS entity_type, count(c) AS support_chunks
            ORDER BY support_chunks DESC
        """, name=disease_name).data()

        return {
            "symptoms": symptoms,
            "drugs": drugs,
            "procedures": procedures,
            "labs": labs,
            "extras": extras,
        }


MAX_SIGNS = 25
MAX_SERVICES = 30       # drugs + procedures combined
MAX_OBSERVATIONS = 15


def build_explorer_graph(icd_code: str, disease_name: str, entities: dict, chunk_count: int) -> dict:
    """Build explorer graph format from protocol entities.

    Limits nodes per category to keep SVG rendering performant (~70 nodes max).
    Entities are already sorted by support_chunks DESC from Neo4j query.
    """
    disease_id = f"disease:{icd_code}"

    # Build signs from symptoms + extras
    signs = []
    for sym in entities["symptoms"]:
        sign_id = f"sign:{_slugify(sym['name'])}"
        signs.append({
            "id": sign_id,
            "type": "sign",
            "label": sym["name"],
            "support_cases": sym["support_chunks"],
            "normalized_key": _slugify(sym["name"]),
        })
    for extra in entities["extras"]:
        sign_id = f"sign:{_slugify(extra['name'])}"
        if not any(s["id"] == sign_id for s in signs):
            signs.append({
                "id": sign_id,
                "type": "sign",
                "label": f"{extra['name']} ({extra.get('entity_type', '')})",
                "support_cases": extra["support_chunks"],
                "normalized_key": _slugify(extra["name"]),
            })

    # Build services from drugs + procedures
    services = []
    for drug in entities["drugs"]:
        code = f"DRUG-{_slugify(drug['name'])[:20]}"
        svc_id = f"service:{icd_code}:{code}"
        services.append({
            "id": svc_id,
            "type": "service",
            "service_code": code,
            "label": drug["name"],
            "category_code": "DRUG",
            "category_name": "Thuốc",
            "roles": ["treatment"],
            "evidences": ["guideline"],
            "max_score": min(1.0, 0.5 + drug["support_chunks"] * 0.05),
            "case_support": drug["support_chunks"],
            "link_count": 1,
            "guideline_hits": drug["support_chunks"],
            "protocol_excel_hits": 0,
            "statistical_hits": 0,
            "max_pmi": 0.0,
            "max_co_occurrence": 0,
            "total_occurrences": drug["support_chunks"],
            "avg_cost_vnd": 0,
            "variants_preview": [drug["name"]],
            "observations": [],
        })

    for proc in entities["procedures"]:
        code = f"PROC-{_slugify(proc['name'])[:20]}"
        svc_id = f"service:{icd_code}:{code}"
        services.append({
            "id": svc_id,
            "type": "service",
            "service_code": code,
            "label": proc["name"],
            "category_code": "PROC",
            "category_name": "Thủ thuật / Chẩn đoán",
            "roles": ["diagnostic"],
            "evidences": ["guideline"],
            "max_score": min(1.0, 0.5 + proc["support_chunks"] * 0.05),
            "case_support": proc["support_chunks"],
            "link_count": 1,
            "guideline_hits": proc["support_chunks"],
            "protocol_excel_hits": 0,
            "statistical_hits": 0,
            "max_pmi": 0.0,
            "max_co_occurrence": 0,
            "total_occurrences": proc["support_chunks"],
            "avg_cost_vnd": 0,
            "variants_preview": [proc["name"]],
            "observations": [],
        })

    # Build observations from lab tests
    observations = []
    for lab in entities["labs"]:
        obs_code = f"OBS-{_slugify(lab['name'])[:20]}"
        observations.append({
            "id": f"disease-observation:{icd_code}:{obs_code}",
            "observation_node_code": obs_code,
            "name": lab["name"],
            "category_code": "LAB-GEN",
            "category_name": "Xét nghiệm",
            "count": lab["support_chunks"],
            "result_flag_counter": {},
            "polarity_counter": {},
            "abnormality_counter": {},
            "sample_results": [],
        })

    # Truncate to top-N per category (already sorted by support_chunks DESC)
    total_before = len(signs) + len(services) + len(observations)
    signs = signs[:MAX_SIGNS]
    services = services[:MAX_SERVICES]
    observations = observations[:MAX_OBSERVATIONS]
    total_after = len(signs) + len(services) + len(observations)
    if total_after < total_before:
        print(f"  [CAP] {disease_name}: {total_before} → {total_after} nodes "
              f"(signs≤{MAX_SIGNS}, services≤{MAX_SERVICES}, obs≤{MAX_OBSERVATIONS})")

    # Build graph nodes & edges
    nodes = [{
        "id": disease_id,
        "type": "disease",
        "label": disease_name,
        "subtitle": icd_code,
        "metrics": {
            "linked_services": len(services),
            "matched_cases": chunk_count,
            "signs": len(signs),
            "observation_nodes": len(observations),
        },
    }]
    edges = []

    for sign in signs:
        nodes.append(sign)
        edges.append({
            "id": f"edge:{disease_id}:{sign['id']}",
            "source": disease_id,
            "target": sign["id"],
            "type": "disease_sign",
            "label": f"dấu hiệu • {sign['support_cases']} chunks",
            "details": {
                "relationship": "Disease -> Sign",
                "support_cases": sign["support_cases"],
                "source": "protocol_guideline",
            },
        })

    for svc in services:
        nodes.append({
            "id": svc["id"],
            "type": "service",
            "label": svc["label"],
            "subtitle": svc["category_name"],
            "service_code": svc["service_code"],
            "category_code": svc["category_code"],
            "category_name": svc["category_name"],
            "max_score": svc["max_score"],
            "case_support": svc["case_support"],
        })
        edges.append({
            "id": f"edge:{disease_id}:{svc['id']}",
            "source": disease_id,
            "target": svc["id"],
            "type": "disease_service",
            "label": f"{svc['category_name']} • {svc['case_support']} chunks",
            "details": {
                "relationship": "Disease -> Service",
                "max_score": svc["max_score"],
                "case_support": svc["case_support"],
                "roles": svc["roles"],
                "source": "protocol_guideline",
            },
        })

    for obs in observations:
        nodes.append({
            "id": obs["id"],
            "type": "observation",
            "label": obs["name"],
            "subtitle": obs["category_name"],
            "observation_node_code": obs["observation_node_code"],
        })
        edges.append({
            "id": f"edge:{disease_id}:{obs['id']}",
            "source": disease_id,
            "target": obs["id"],
            "type": "disease_observation",
            "label": f"xét nghiệm • {obs['count']} chunks",
            "details": {
                "relationship": "Disease -> Observation",
                "count": obs["count"],
                "source": "protocol_guideline",
            },
        })

    summary = {
        "icd10": icd_code,
        "disease_name": disease_name,
        "icd_group": icd_code,
        "case_count": chunk_count,
        "message_count": 0,
        "linked_service_count": len(services),
        "sign_count": len(signs),
        "disease_observation_count": len(observations),
        "top_hospitals": [],
        "top_departments": [],
        "diagnosis_examples": [],
        "sample_case_ids": [],
    }

    return {
        "summary": summary,
        "nodes": nodes,
        "edges": edges,
        "signs": signs,
        "services": services,
        "disease_observations": observations,
    }


def merge_into_bundle(bundle: dict, disease_id: str, graph: dict, disease_stub: dict) -> None:
    """Merge a protocol-derived graph into the existing bundle."""
    # Update or add disease_index entry
    idx = next((i for i, d in enumerate(bundle["disease_index"]) if d["disease_id"] == disease_id), None)

    if idx is not None:
        # Existing disease — enrich with protocol data
        existing = bundle["disease_index"][idx]
        # Add protocol sign/service counts if higher
        existing["linked_service_count"] = max(
            existing.get("linked_service_count", 0),
            disease_stub["linked_service_count"],
        )
        existing["sign_count"] = max(
            existing.get("sign_count", 0),
            disease_stub["sign_count"],
        )
        existing["disease_observation_count"] = max(
            existing.get("disease_observation_count", 0),
            disease_stub["disease_observation_count"],
        )

        # Merge graph: add protocol signs/services/observations to existing
        existing_graph = bundle.get("graphs", {}).get(disease_id, {})
        if existing_graph:
            # Merge signs
            existing_sign_ids = {s["id"] for s in existing_graph.get("signs", [])}
            for sign in graph["signs"]:
                if sign["id"] not in existing_sign_ids:
                    existing_graph["signs"].append(sign)

            # Merge services
            existing_svc_ids = {s["id"] for s in existing_graph.get("services", [])}
            for svc in graph["services"]:
                if svc["id"] not in existing_svc_ids:
                    existing_graph["services"].append(svc)

            # Merge observations
            existing_obs_ids = {o["id"] for o in existing_graph.get("disease_observations", [])}
            for obs in graph["disease_observations"]:
                if obs["id"] not in existing_obs_ids:
                    existing_graph["disease_observations"].append(obs)

            # Merge nodes & edges
            existing_node_ids = {n["id"] for n in existing_graph.get("nodes", [])}
            for node in graph["nodes"]:
                if node["id"] not in existing_node_ids:
                    existing_graph["nodes"].append(node)

            existing_edge_ids = {e["id"] for e in existing_graph.get("edges", [])}
            for edge in graph["edges"]:
                if edge["id"] not in existing_edge_ids:
                    existing_graph["edges"].append(edge)

            # Cap merged lists to keep graph renderable
            existing_graph["signs"] = existing_graph.get("signs", [])[:MAX_SIGNS]
            existing_graph["services"] = existing_graph.get("services", [])[:MAX_SERVICES]
            existing_graph["disease_observations"] = existing_graph.get("disease_observations", [])[:MAX_OBSERVATIONS]

            # Rebuild nodes/edges from capped lists
            allowed_ids = {n["id"] for cat in ["signs", "services", "disease_observations"]
                           for n in existing_graph.get(cat, [])}
            allowed_ids.add(disease_id)  # keep disease node
            existing_graph["nodes"] = [n for n in existing_graph.get("nodes", []) if n["id"] in allowed_ids]
            existing_graph["edges"] = [e for e in existing_graph.get("edges", [])
                                       if e["source"] in allowed_ids and e["target"] in allowed_ids]

            # Update summary metrics
            existing_graph["summary"]["linked_service_count"] = len(existing_graph.get("services", []))
            existing_graph["summary"]["sign_count"] = len(existing_graph.get("signs", []))
            existing_graph["summary"]["disease_observation_count"] = len(existing_graph.get("disease_observations", []))

            bundle["graphs"][disease_id] = existing_graph
        else:
            bundle.setdefault("graphs", {})[disease_id] = graph
    else:
        # New disease — add fresh
        bundle["disease_index"].append(disease_stub)
        bundle.setdefault("graphs", {})[disease_id] = graph


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Sync protocol entities into Disease Explorer")
    parser.add_argument("--icd", help="Sync only this ICD code")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change, don't write")
    parser.add_argument("--import-neo4j", action="store_true",
                        help="Also import into Neo4j claims_insights namespace")
    args = parser.parse_args()

    driver = get_driver()

    # 1. Get all protocol diseases from Neo4j
    print("[1/4] Querying protocol diseases from Neo4j...")
    diseases = query_protocol_diseases(driver)
    if args.icd:
        diseases = [d for d in diseases if d["icd_code"] == args.icd.upper()]

    if not diseases:
        print("  No protocol diseases found.")
        return

    print(f"  Found {len(diseases)} diseases with protocol data")
    for d in diseases:
        print(f"    [{d['icd_code']}] {d['disease_name']} — {d['chunk_count']} chunks")

    # 2. Query entities for each disease
    print(f"\n[2/4] Querying entities for {len(diseases)} diseases...")
    disease_graphs = {}
    for d in diseases:
        entities = query_disease_entities(driver, d["disease_name"])
        total_entities = sum(len(v) for v in entities.values())
        print(f"  [{d['icd_code']}] {d['disease_name']}: "
              f"{len(entities['symptoms'])} symptoms, {len(entities['drugs'])} drugs, "
              f"{len(entities['procedures'])} procedures, {len(entities['labs'])} labs "
              f"({total_entities} total)")

        graph = build_explorer_graph(
            d["icd_code"], d["disease_name"], entities, d["chunk_count"],
        )
        disease_graphs[d["icd_code"]] = {
            "graph": graph,
            "stub": {
                "disease_id": f"disease:{d['icd_code']}",
                "icd10": d["icd_code"],
                "icd_group": d["icd_code"],
                "disease_name": d["disease_name"],
                "case_count": d["chunk_count"],
                "message_count": 0,
                "linked_service_count": len(graph["services"]),
                "sign_count": len(graph["signs"]),
                "disease_observation_count": len(graph["disease_observations"]),
                "top_hospital": "BYT (Phác đồ)",
            },
        }

    driver.close()

    if args.dry_run:
        print(f"\n[DRY RUN] Would merge {len(disease_graphs)} diseases into explorer bundle")
        for icd, data in disease_graphs.items():
            g = data["graph"]
            print(f"  [{icd}] {len(g['signs'])} signs, {len(g['services'])} services, "
                  f"{len(g['disease_observations'])} observations, "
                  f"{len(g['nodes'])} nodes, {len(g['edges'])} edges")
        return

    # 3. Load existing bundle and merge
    print(f"\n[3/4] Loading existing bundle: {BUNDLE_PATH.name}")
    if BUNDLE_PATH.exists():
        with open(BUNDLE_PATH, "r", encoding="utf-8") as f:
            bundle = json.load(f)
        print(f"  Existing: {len(bundle.get('disease_index', []))} diseases, "
              f"{len(bundle.get('graphs', {}))} graphs")
    else:
        bundle = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "stats": {},
            "disease_index": [],
            "graphs": {},
        }
        print("  No existing bundle — creating new")

    for icd, data in disease_graphs.items():
        disease_id = f"disease:{icd}"
        merge_into_bundle(bundle, disease_id, data["graph"], data["stub"])
        print(f"  Merged [{icd}] {data['stub']['disease_name']}")

    # Update stats
    bundle["generated_at"] = datetime.now(timezone.utc).isoformat()
    bundle["stats"]["disease_count"] = len(bundle["disease_index"])
    total_svc = sum(len(g.get("services", [])) for g in bundle.get("graphs", {}).values())
    total_sign = sum(len(g.get("signs", [])) for g in bundle.get("graphs", {}).values())
    total_obs = sum(len(g.get("disease_observations", [])) for g in bundle.get("graphs", {}).values())
    bundle["stats"]["service_link_count"] = total_svc
    bundle["stats"]["sign_link_count"] = total_sign
    bundle["stats"]["disease_observation_count"] = total_obs

    # 4. Save bundle
    print(f"\n[4/4] Saving updated bundle...")
    with open(BUNDLE_PATH, "w", encoding="utf-8") as f:
        json.dump(bundle, f, ensure_ascii=False, indent=2)
    print(f"  Saved: {BUNDLE_PATH}")
    print(f"  Total: {bundle['stats']['disease_count']} diseases, "
          f"{total_svc} services, {total_sign} signs, {total_obs} observations")

    # Optional: import to Neo4j
    if args.import_neo4j:
        print(f"\n[Extra] Importing to Neo4j claims_insights namespace...")
        from server_support.claims_insights_graph_store import ClaimsInsightsGraphStore
        store = ClaimsInsightsGraphStore(bundle_path=BUNDLE_PATH)
        result = store.import_bundle(bundle, clear_existing=True)
        print(f"  Imported: {result}")
        store.close()


if __name__ == "__main__":
    main()
