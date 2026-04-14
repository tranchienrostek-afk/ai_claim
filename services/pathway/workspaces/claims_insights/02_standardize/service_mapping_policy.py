from __future__ import annotations

from typing import Any


def resolve_mapping_resolution(suggestions: list[dict[str, Any]]) -> dict[str, Any]:
    if not suggestions:
        return {
            "mapping_resolution": "unknown",
            "mapping_gap": 0.0,
            "mapping_reason": "No mapping candidates were produced by the service mapper.",
            "accepted": False,
            "suggested_service_code": "",
            "suggested_canonical_name": "",
            "mapper_score": 0.0,
            "mapper_confidence": "NONE",
            "top_candidates": [],
            "mapping_status": "unmapped",
        }

    top = suggestions[0]
    top_score = float(top.get("score", 0.0) or 0.0)
    second_score = float((suggestions[1] or {}).get("score", 0.0) or 0.0) if len(suggestions) > 1 else 0.0
    gap = round(top_score - second_score, 2)
    mapper_confidence = str(top.get("confidence") or "REVIEW").upper()
    reasons = set(top.get("reasons") or [])
    exact_like = "exact_cleaned_match" in reasons or "exact_after_noise_strip" in reasons

    if exact_like and top_score >= 95:
        resolution = "exact"
        reason = "Exact or near-exact normalized service match."
    elif mapper_confidence == "HIGH" and top_score >= 93 and gap >= 4:
        resolution = "exact"
        reason = "High-confidence mapper result with clear separation from alternatives."
    elif mapper_confidence in {"HIGH", "MEDIUM"} and top_score >= 87 and gap >= 2.5:
        resolution = "probable"
        reason = "Top candidate is materially stronger than nearby alternatives."
    elif top_score >= 80:
        resolution = "ambiguous"
        reason = "Top candidate exists but is not separated enough to lock a service_code safely."
    else:
        resolution = "unknown"
        reason = "Mapper score is too weak to trust a standardized service_code."

    accepted = resolution in {"exact", "probable"}
    if accepted:
        mapping_status = "mapped"
    elif resolution == "ambiguous":
        mapping_status = "review"
    else:
        mapping_status = "unmapped"

    top_candidates = [
        {
            "service_code": str(item.get("service_code") or ""),
            "canonical_name": str(item.get("canonical_name") or ""),
            "category_code": str(item.get("category_code") or ""),
            "category_name": str(item.get("category_name") or ""),
            "score": float(item.get("score", 0.0) or 0.0),
            "confidence": str(item.get("confidence") or ""),
            "matched_variant": str(item.get("matched_variant") or ""),
        }
        for item in suggestions[:3]
    ]

    return {
        "mapping_resolution": resolution,
        "mapping_gap": gap,
        "mapping_reason": reason,
        "accepted": accepted,
        "suggested_service_code": str(top.get("service_code") or ""),
        "suggested_canonical_name": str(top.get("canonical_name") or ""),
        "mapper_score": round(top_score, 2),
        "mapper_confidence": mapper_confidence,
        "top_candidates": top_candidates,
        "mapping_status": mapping_status,
    }
