from __future__ import annotations

import html
import json
from typing import Any


class PathwayKnowledgeTextBridge:
    """Editable text workspace plus graph-backed highlighting for protocol assets."""

    def __init__(self, ontology_store, knowledge_registry_store):
        self.ontology_store = ontology_store
        self.knowledge_registry_store = knowledge_registry_store

    def get_asset_text_view(
        self,
        asset_id: str,
        *,
        refresh_source: bool = False,
    ) -> dict[str, Any]:
        workspace = self.knowledge_registry_store.get_text_workspace(
            asset_id,
            create_if_missing=True,
            force_refresh=refresh_source,
        )
        graph_trace = self.knowledge_registry_store.graph_trace(asset_id)
        doc_ids = [row.get("doc_id") for row in graph_trace.get("ontology_documents") or [] if row.get("doc_id")]
        manual_labels = self.knowledge_registry_store.list_manual_labels(asset_id)
        annotations = self._build_annotations(workspace.get("content") or "", doc_ids, manual_labels=manual_labels)
        return {
            **workspace,
            "graph_trace": graph_trace,
            "annotations": annotations["annotations"],
            "annotation_summary": annotations["summary"],
            "annotated_html": annotations["annotated_html"],
            "entity_cards": annotations["entity_cards"],
            "manual_labels": manual_labels,
            "manual_label_summary": self._manual_label_summary(manual_labels),
        }

    def save_asset_text_view(self, asset_id: str, content: str, note: str = "") -> dict[str, Any]:
        workspace = self.knowledge_registry_store.save_text_workspace(asset_id, content, note=note)
        graph_trace = self.knowledge_registry_store.graph_trace(asset_id)
        doc_ids = [row.get("doc_id") for row in graph_trace.get("ontology_documents") or [] if row.get("doc_id")]
        manual_labels = self.knowledge_registry_store.list_manual_labels(asset_id)
        annotations = self._build_annotations(workspace.get("content") or "", doc_ids, manual_labels=manual_labels)
        return {
            **workspace,
            "graph_trace": graph_trace,
            "annotations": annotations["annotations"],
            "annotation_summary": annotations["summary"],
            "annotated_html": annotations["annotated_html"],
            "entity_cards": annotations["entity_cards"],
            "manual_labels": manual_labels,
            "manual_label_summary": self._manual_label_summary(manual_labels),
        }

    def upsert_manual_label(self, asset_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        label = self.knowledge_registry_store.upsert_manual_label(asset_id, payload or {})
        self.apply_manual_labels_to_graph(asset_id)
        view = self.get_asset_text_view(asset_id)
        view["last_manual_label"] = label
        return view

    def replace_manual_labels(self, asset_id: str, labels: list[dict[str, Any]]) -> dict[str, Any]:
        normalized = self.knowledge_registry_store.replace_manual_labels(asset_id, labels or [])
        self.apply_manual_labels_to_graph(asset_id)
        view = self.get_asset_text_view(asset_id)
        view["manual_labels"] = normalized
        view["manual_label_summary"] = self._manual_label_summary(normalized)
        return view

    def delete_manual_label(self, asset_id: str, label_id: str) -> dict[str, Any]:
        self.knowledge_registry_store.delete_manual_label(asset_id, label_id)
        self.apply_manual_labels_to_graph(asset_id)
        return self.get_asset_text_view(asset_id)

    def apply_manual_labels_to_graph(self, asset_id: str) -> dict[str, Any]:
        graph_trace = self.knowledge_registry_store.graph_trace(asset_id)
        doc_ids = [row.get("doc_id") for row in graph_trace.get("ontology_documents") or [] if row.get("doc_id")]
        manual_labels = self.knowledge_registry_store.list_manual_labels(asset_id)
        if not doc_ids:
            return {"asset_id": asset_id, "applied": 0, "deleted": 0, "reason": "no_graph_documents"}
        chunks = self._chunk_index(doc_ids)
        keep_ids = [str(item.get("mention_id") or "") for item in manual_labels if str(item.get("mention_id") or "").strip()]
        with self.ontology_store.driver.session() as session:
            deleted = session.run(
                """
                MATCH (m)
                WHERE m.manual_override = true
                  AND m.asset_id = $asset_id
                  AND (m:RawSignMention OR m:RawServiceMention OR m:RawObservationMention)
                  AND NOT m.mention_id IN $keep_ids
                DETACH DELETE m
                RETURN count(m) AS deleted_count
                """,
                asset_id=asset_id,
                keep_ids=keep_ids or [""],
            ).single()
            applied = 0
            for label in manual_labels:
                chunk = self._pick_chunk(chunks, label)
                self._materialize_manual_label(session, asset_id, label, chunk)
                applied += 1
        return {
            "asset_id": asset_id,
            "applied": applied,
            "deleted": int((deleted or {}).get("deleted_count") or 0),
            "doc_count": len(doc_ids),
        }

    def _build_annotations(
        self,
        text: str,
        doc_ids: list[str],
        *,
        manual_labels: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        graph_candidates = self._annotation_candidates(doc_ids)
        manual_candidates = self._manual_candidates(manual_labels or [])
        candidates = manual_candidates + graph_candidates
        annotations = self._match_annotations(text, candidates)
        summary = {
            "candidate_count": len(candidates),
            "matched_count": len(annotations),
            "graph_candidate_count": len(graph_candidates),
            "manual_candidate_count": len(manual_candidates),
            "by_kind": {},
        }
        for ann in annotations:
            kind = ann.get("kind") or "unknown"
            summary["by_kind"][kind] = summary["by_kind"].get(kind, 0) + 1
        annotated_html = self._annotated_html(text, annotations)
        entity_cards = [self._entity_card(candidate) for candidate in candidates[:120]]
        return {
            "annotations": annotations,
            "summary": summary,
            "annotated_html": annotated_html,
            "entity_cards": entity_cards,
        }

    def _annotation_candidates(self, doc_ids: list[str]) -> list[dict[str, Any]]:
        if not doc_ids:
            return []
        with self.ontology_store.driver.session() as session:
            sign_rows = [
                dict(row)
                for row in session.run(
                    """
                    MATCH (doc:RawDocument)
                    WHERE doc.doc_id IN $doc_ids
                    MATCH (doc)<-[:FROM_DOCUMENT]-(chunk:RawChunk)-[:MENTIONS_SIGN]->(m:RawSignMention)
                    OPTIONAL MATCH (m)-[:MAPS_TO_SIGN]->(sc)
                    OPTIONAL MATCH (chunk)-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity)
                    RETURN 'sign' AS kind,
                           m.mention_id AS entity_id,
                           m.mention_text AS term,
                           m.mapping_status AS mapping_status,
                           m.source_page AS source_page,
                           coalesce(sc.canonical_label, sc.sign_id, '') AS concept_label,
                           coalesce(sc.sign_id, '') AS concept_id,
                           d.disease_name AS disease_name,
                           chunk.section_title AS section_title,
                           m.context_text AS context_text
                    ORDER BY m.mention_id
                    """,
                    doc_ids=doc_ids,
                )
            ]
            service_rows = [
                dict(row)
                for row in session.run(
                    """
                    MATCH (doc:RawDocument)
                    WHERE doc.doc_id IN $doc_ids
                    MATCH (doc)<-[:FROM_DOCUMENT]-(chunk:RawChunk)-[:MENTIONS_SERVICE]->(m:RawServiceMention)
                    OPTIONAL MATCH (m)-[:MAPS_TO_SERVICE]->(svc)
                    OPTIONAL MATCH (chunk)-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity)
                    RETURN 'service' AS kind,
                           m.mention_id AS entity_id,
                           m.mention_text AS term,
                           m.mapping_status AS mapping_status,
                           m.source_page AS source_page,
                           coalesce(svc.service_name, svc.name, '') AS concept_label,
                           coalesce(svc.service_code, '') AS concept_id,
                           d.disease_name AS disease_name,
                           chunk.section_title AS section_title,
                           m.context_text AS context_text,
                           m.medical_role AS medical_role,
                           m.condition_to_apply AS condition_to_apply
                    ORDER BY m.mention_id
                    """,
                    doc_ids=doc_ids,
                )
            ]
            observation_rows = [
                dict(row)
                for row in session.run(
                    """
                    MATCH (doc:RawDocument)
                    WHERE doc.doc_id IN $doc_ids
                    MATCH (doc)<-[:FROM_DOCUMENT]-(chunk:RawChunk)-[:MENTIONS_OBSERVATION]->(m:RawObservationMention)
                    OPTIONAL MATCH (chunk)-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity)
                    RETURN 'observation' AS kind,
                           m.mention_id AS entity_id,
                           m.mention_text AS term,
                           m.mapping_status AS mapping_status,
                           m.source_page AS source_page,
                           coalesce(m.result_semantics, '') AS concept_label,
                           '' AS concept_id,
                           d.disease_name AS disease_name,
                           chunk.section_title AS section_title,
                           m.context_text AS context_text
                    ORDER BY m.mention_id
                    """,
                    doc_ids=doc_ids,
                )
            ]
        candidates: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in sign_rows + service_rows + observation_rows:
            term = str(row.get("term") or "").strip()
            if len(term) < 2:
                continue
            detail = self._detail_lines(row)
            key = json.dumps(
                [
                    row.get("kind"),
                    self._ascii_fold(term),
                    row.get("concept_id"),
                    row.get("concept_label"),
                    row.get("medical_role"),
                ],
                ensure_ascii=True,
            )
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                {
                    "kind": row.get("kind") or "unknown",
                    "entity_id": row.get("entity_id") or "",
                    "term": term,
                    "term_folded": self._ascii_fold(term),
                    "detail": detail,
                    "tooltip": "\n".join(detail),
                    "concept_id": row.get("concept_id") or "",
                    "concept_label": row.get("concept_label") or "",
                    "disease_name": row.get("disease_name") or "",
                    "section_title": row.get("section_title") or "",
                    "source_page": row.get("source_page"),
                }
            )
        candidates.sort(key=lambda item: (-len(item.get("term") or ""), item.get("kind") or ""))
        return candidates

    def _manual_candidates(self, manual_labels: list[dict[str, Any]]) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        for row in manual_labels or []:
            term = str(row.get("text") or "").strip()
            if len(term) < 1:
                continue
            detail = [
                f"Loai: {row.get('kind') or 'unknown'}",
                f"Text: {term}",
                "Nguon: manual_label",
            ]
            if row.get("concept_label"):
                detail.append(f"Graph: {row.get('concept_label')}")
            if row.get("concept_ref"):
                detail.append(f"ID: {row.get('concept_ref')}")
            if row.get("source_chunk_id"):
                detail.append(f"Chunk: {row.get('source_chunk_id')}")
            if row.get("source_page") not in (None, ""):
                detail.append(f"Trang: {row.get('source_page')}")
            if row.get("note"):
                detail.append(f"Ghi chu: {row.get('note')}")
            candidates.append(
                {
                    "kind": row.get("kind") or "unknown",
                    "entity_id": row.get("mention_id") or row.get("label_id") or "",
                    "label_id": row.get("label_id") or "",
                    "term": term,
                    "term_folded": self._ascii_fold(term),
                    "detail": detail,
                    "tooltip": "\n".join(detail),
                    "concept_id": row.get("concept_ref") or "",
                    "concept_label": row.get("concept_label") or "",
                    "disease_name": "",
                    "section_title": "",
                    "source_page": row.get("source_page"),
                    "manual": True,
                    "note": row.get("note") or "",
                }
            )
        candidates.sort(key=lambda item: (-len(item.get("term") or ""), item.get("kind") or ""))
        return candidates

    def _manual_label_summary(self, manual_labels: list[dict[str, Any]]) -> dict[str, Any]:
        summary = {"count": len(manual_labels or []), "by_kind": {}}
        for label in manual_labels or []:
            kind = str(label.get("kind") or "unknown")
            summary["by_kind"][kind] = summary["by_kind"].get(kind, 0) + 1
        return summary

    def _detail_lines(self, row: dict[str, Any]) -> list[str]:
        lines = [f"Loai: {row.get('kind') or 'unknown'}", f"Text: {row.get('term') or ''}"]
        if row.get("concept_label"):
            lines.append(f"Graph: {row.get('concept_label')}")
        if row.get("concept_id"):
            lines.append(f"ID: {row.get('concept_id')}")
        if row.get("medical_role"):
            lines.append(f"Vai tro: {row.get('medical_role')}")
        if row.get("condition_to_apply"):
            lines.append(f"Dieu kien: {row.get('condition_to_apply')}")
        if row.get("mapping_status"):
            lines.append(f"Mapping: {row.get('mapping_status')}")
        if row.get("disease_name"):
            lines.append(f"Benh: {row.get('disease_name')}")
        if row.get("section_title"):
            lines.append(f"Muc: {row.get('section_title')}")
        if row.get("source_page") not in (None, ""):
            lines.append(f"Trang: {row.get('source_page')}")
        if row.get("context_text"):
            context = str(row.get("context_text") or "").strip().replace("\n", " ")
            lines.append(f"Context: {context[:180]}")
        if row.get("manual"):
            lines.append("Nguon: manual_label")
        if row.get("note"):
            lines.append(f"Ghi chu: {row.get('note')}")
        return lines

    def _match_annotations(self, text: str, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not text.strip() or not candidates:
            return []
        text_folded = self._ascii_fold(text)
        consumed: list[tuple[int, int]] = []
        results: list[dict[str, Any]] = []

        for candidate in candidates:
            term_folded = candidate.get("term_folded") or ""
            if len(term_folded) < 2:
                continue
            start = 0
            while True:
                idx = text_folded.find(term_folded, start)
                if idx < 0:
                    break
                end = idx + len(term_folded)
                if not any(idx < used_end and end > used_start for used_start, used_end in consumed):
                    snippet = text[idx:end]
                    results.append(
                        {
                            "start": idx,
                            "end": end,
                            "text": snippet,
                            "kind": candidate.get("kind") or "unknown",
                            "entity_id": candidate.get("entity_id") or "",
                            "tooltip": candidate.get("tooltip") or "",
                            "detail": candidate.get("detail") or [],
                            "concept_id": candidate.get("concept_id") or "",
                            "concept_label": candidate.get("concept_label") or "",
                            "disease_name": candidate.get("disease_name") or "",
                            "section_title": candidate.get("section_title") or "",
                            "source_page": candidate.get("source_page"),
                            "manual": bool(candidate.get("manual")),
                            "label_id": candidate.get("label_id") or "",
                        }
                    )
                    consumed.append((idx, end))
                start = idx + max(1, len(term_folded))

        results.sort(key=lambda item: item["start"])
        return results

    def _annotated_html(self, text: str, annotations: list[dict[str, Any]]) -> str:
        if not text:
            return '<div class="empty">Chua co text workspace.</div>'
        if not annotations:
            return f'<pre class="text-preview-plain">{html.escape(text)}</pre>'

        parts: list[str] = []
        cursor = 0
        for ann in annotations:
            start = ann["start"]
            end = ann["end"]
            if start > cursor:
                parts.append(html.escape(text[cursor:start]))
            css_class = f"entity-mark entity-{ann.get('kind') or 'unknown'}"
            if ann.get("manual"):
                css_class += " entity-manual"
            tooltip = html.escape(ann.get("tooltip") or "", quote=True)
            body = html.escape(text[start:end])
            parts.append(
                f'<mark class="{css_class}" title="{tooltip}" data-kind="{html.escape(ann.get("kind") or "")}">{body}</mark>'
            )
            cursor = end
        if cursor < len(text):
            parts.append(html.escape(text[cursor:]))
        return f'<pre class="text-preview-plain">{"".join(parts)}</pre>'

    def _entity_card(self, candidate: dict[str, Any]) -> dict[str, Any]:
        return {
            "kind": candidate.get("kind") or "unknown",
            "term": candidate.get("term") or "",
            "concept_label": candidate.get("concept_label") or "",
            "concept_id": candidate.get("concept_id") or "",
            "detail": candidate.get("detail") or [],
            "manual": bool(candidate.get("manual")),
            "label_id": candidate.get("label_id") or "",
        }

    def _ascii_fold(self, text: str) -> str:
        return str(text or "").lower()

    def _chunk_index(self, doc_ids: list[str]) -> list[dict[str, Any]]:
        if not doc_ids:
            return []
        with self.ontology_store.driver.session() as session:
            rows = [
                dict(row)
                for row in session.run(
                    """
                    MATCH (doc:RawDocument)
                    WHERE doc.doc_id IN $doc_ids
                    MATCH (doc)<-[:FROM_DOCUMENT]-(chunk:RawChunk)
                    RETURN chunk.chunk_id AS chunk_id,
                           chunk.namespace AS namespace,
                           chunk.section_title AS section_title,
                           chunk.body_text AS body_text,
                           coalesce(chunk.page_numbers, []) AS page_numbers
                    ORDER BY size(coalesce(chunk.body_text, '')) ASC, chunk.chunk_id
                    """,
                    doc_ids=doc_ids,
                )
            ]
        for row in rows:
            row["body_folded"] = self._ascii_fold(row.get("body_text") or "")
        return rows

    def _pick_chunk(self, chunks: list[dict[str, Any]], label: dict[str, Any]) -> dict[str, Any]:
        explicit_chunk_id = str(label.get("source_chunk_id") or "").strip()
        if explicit_chunk_id:
            for chunk in chunks:
                if str(chunk.get("chunk_id") or "") == explicit_chunk_id:
                    selected = dict(chunk)
                    selected["chunk_match_mode"] = "explicit"
                    return selected
        term_folded = self._ascii_fold(label.get("text") or "")
        if term_folded:
            for chunk in chunks:
                if term_folded in str(chunk.get("body_folded") or ""):
                    selected = dict(chunk)
                    selected["chunk_match_mode"] = "contains"
                    return selected
        if chunks:
            selected = dict(chunks[0])
            selected["chunk_match_mode"] = "fallback_first"
            return selected
        return {
            "chunk_id": "",
            "namespace": "",
            "section_title": "",
            "page_numbers": [],
            "chunk_match_mode": "missing",
        }

    def _materialize_manual_label(self, session, asset_id: str, label: dict[str, Any], chunk: dict[str, Any]) -> None:
        kind = str(label.get("kind") or "").strip().lower()
        mention_id = str(label.get("mention_id") or "").strip()
        if not mention_id or kind not in {"sign", "service", "observation"}:
            return
        label_name = {
            "sign": "RawSignMention",
            "service": "RawServiceMention",
            "observation": "RawObservationMention",
        }[kind]
        chunk_rel = {
            "sign": "MENTIONS_SIGN",
            "service": "MENTIONS_SERVICE",
            "observation": "MENTIONS_OBSERVATION",
        }[kind]
        page_numbers = chunk.get("page_numbers") or []
        page_value = label.get("source_page")
        if page_value in (None, "") and page_numbers:
            page_value = page_numbers[0]
        params = {
            "mention_id": mention_id,
            "namespace": chunk.get("namespace") or "",
            "mention_text": label.get("text") or "",
            "context_text": label.get("text") or "",
            "result_semantics": label.get("concept_label") or label.get("concept_ref") or "",
            "extraction_confidence": 1.0,
            "mapping_status": "manual_override",
            "source_chunk_id": chunk.get("chunk_id") or "",
            "source_page": page_value,
            "manual_label_id": label.get("label_id") or "",
            "manual_note": label.get("note") or "",
            "manual_status": label.get("manual_status") or "active",
            "asset_id": asset_id,
            "chunk_match_mode": chunk.get("chunk_match_mode") or "missing",
            "medical_role": "",
            "condition_to_apply": "",
            "modifier_raw": "",
            "concept_ref": label.get("concept_ref") or "",
            "concept_label": label.get("concept_label") or "",
            "created_at": label.get("created_at") or "",
            "updated_at": label.get("updated_at") or "",
        }
        if kind == "sign":
            session.run(
                """
                MERGE (m:RawSignMention {mention_id:$mention_id})
                SET m.namespace = $namespace,
                    m.mention_text = $mention_text,
                    m.context_text = $context_text,
                    m.modifier_raw = $modifier_raw,
                    m.extraction_confidence = $extraction_confidence,
                    m.mapping_status = $mapping_status,
                    m.source_chunk_id = $source_chunk_id,
                    m.source_page = $source_page,
                    m.manual_override = true,
                    m.manual_label_id = $manual_label_id,
                    m.manual_note = $manual_note,
                    m.manual_status = $manual_status,
                    m.asset_id = $asset_id,
                    m.chunk_match_mode = $chunk_match_mode,
                    m.created_at = coalesce(m.created_at, $created_at),
                    m.updated_at = $updated_at
                """,
                **params,
            )
            if params["source_chunk_id"]:
                session.run(
                    f"""
                    MATCH (c:RawChunk {{chunk_id:$chunk_id}})
                    MATCH (m:{label_name} {{mention_id:$mention_id}})
                    MERGE (c)-[:{chunk_rel}]->(m)
                    """,
                    chunk_id=params["source_chunk_id"],
                    mention_id=mention_id,
                )
            session.run("MATCH (m:RawSignMention {mention_id:$mention_id})-[r:MAPS_TO_SIGN]->() DELETE r", mention_id=mention_id)
            if params["concept_ref"] or params["concept_label"]:
                session.run(
                    """
                    MATCH (m:RawSignMention {mention_id:$mention_id})
                    MATCH (s)
                    WHERE (s:SignConcept OR s:ClaimSignConcept OR s:CISign)
                      AND (
                        ($concept_ref <> '' AND coalesce(s.sign_id, s.claim_sign_id, s.canonical_label, s.text) = $concept_ref)
                        OR ($concept_ref = '' AND $concept_label <> '' AND coalesce(s.canonical_label, s.text, s.sign_id, s.claim_sign_id) = $concept_label)
                      )
                    WITH m, s LIMIT 1
                    MERGE (m)-[:MAPS_TO_SIGN]->(s)
                    """,
                    mention_id=mention_id,
                    concept_ref=params["concept_ref"],
                    concept_label=params["concept_label"],
                )
            return
        if kind == "service":
            session.run(
                """
                MERGE (m:RawServiceMention {mention_id:$mention_id})
                SET m.namespace = $namespace,
                    m.mention_text = $mention_text,
                    m.context_text = $context_text,
                    m.medical_role = $medical_role,
                    m.condition_to_apply = $condition_to_apply,
                    m.extraction_confidence = $extraction_confidence,
                    m.mapping_status = $mapping_status,
                    m.source_chunk_id = $source_chunk_id,
                    m.source_page = $source_page,
                    m.manual_override = true,
                    m.manual_label_id = $manual_label_id,
                    m.manual_note = $manual_note,
                    m.manual_status = $manual_status,
                    m.asset_id = $asset_id,
                    m.chunk_match_mode = $chunk_match_mode,
                    m.created_at = coalesce(m.created_at, $created_at),
                    m.updated_at = $updated_at
                """,
                **params,
            )
            if params["source_chunk_id"]:
                session.run(
                    f"""
                    MATCH (c:RawChunk {{chunk_id:$chunk_id}})
                    MATCH (m:{label_name} {{mention_id:$mention_id}})
                    MERGE (c)-[:{chunk_rel}]->(m)
                    """,
                    chunk_id=params["source_chunk_id"],
                    mention_id=mention_id,
                )
            session.run("MATCH (m:RawServiceMention {mention_id:$mention_id})-[r:MAPS_TO_SERVICE]->() DELETE r", mention_id=mention_id)
            if params["concept_ref"] or params["concept_label"]:
                session.run(
                    """
                    MATCH (m:RawServiceMention {mention_id:$mention_id})
                    MATCH (svc)
                    WHERE (svc:ProtocolService OR svc:CIService)
                      AND (
                            ($concept_ref <> '' AND svc.service_code = $concept_ref)
                         OR ($concept_ref = '' AND $concept_label <> '' AND coalesce(svc.service_name, svc.name) = $concept_label)
                      )
                    WITH m, svc LIMIT 1
                    MERGE (m)-[:MAPS_TO_SERVICE]->(svc)
                    """,
                    mention_id=mention_id,
                    concept_ref=params["concept_ref"],
                    concept_label=params["concept_label"],
                )
            return
        session.run(
            """
            MERGE (m:RawObservationMention {mention_id:$mention_id})
            SET m.namespace = $namespace,
                m.mention_text = $mention_text,
                m.context_text = $context_text,
                m.result_semantics = $result_semantics,
                m.extraction_confidence = $extraction_confidence,
                m.mapping_status = $mapping_status,
                m.source_chunk_id = $source_chunk_id,
                m.source_page = $source_page,
                m.manual_override = true,
                m.manual_label_id = $manual_label_id,
                m.manual_note = $manual_note,
                m.manual_status = $manual_status,
                m.asset_id = $asset_id,
                m.chunk_match_mode = $chunk_match_mode,
                m.created_at = coalesce(m.created_at, $created_at),
                m.updated_at = $updated_at
            """,
            **params,
        )
        if params["source_chunk_id"]:
            session.run(
                f"""
                MATCH (c:RawChunk {{chunk_id:$chunk_id}})
                MATCH (m:{label_name} {{mention_id:$mention_id}})
                MERGE (c)-[:{chunk_rel}]->(m)
                """,
                chunk_id=params["source_chunk_id"],
                mention_id=mention_id,
            )
