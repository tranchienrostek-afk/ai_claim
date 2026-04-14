"""Anomaly Detection Agent — flags pricing, duplicate, and quantity anomalies.

This agent does not approve or deny on its own. It raises flags that the
orchestrator uses to downgrade approval to review or strengthen a deny signal.
"""

from __future__ import annotations

import logging
import unicodedata
from collections import Counter
from typing import Any

from .models import AgentVerdict, EvidenceItem, ServiceLineInput

logger = logging.getLogger(__name__)

_PRICE_OUTLIER_RATIO = 3.0
_PRICE_EXTREME_RATIO = 5.0
_HIGH_QUANTITY_THRESHOLD = 5


def _strip_diacritics(text: str) -> str:
    text = text.lower().replace("đ", "d").replace("Đ", "d")
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


class AnomalyAgent:
    """Specialist agent for fraud and anomaly detection."""

    agent_name: str = "anomaly"

    def assess(
        self,
        line: ServiceLineInput,
        service_info: dict[str, Any],
        all_lines: list[ServiceLineInput],
    ) -> AgentVerdict:
        """Check for pricing, duplicate, and quantity anomalies."""
        flags: list[str] = []
        evidence: list[EvidenceItem] = []

        # 1. Price anomaly vs BHYT reference
        bhyt_price = service_info.get("bhyt_price")
        if bhyt_price and bhyt_price > 0 and line.cost_vnd > 0:
            ratio = line.cost_vnd / bhyt_price
            if ratio > _PRICE_EXTREME_RATIO:
                flags.append("price_extreme")
                evidence.append(EvidenceItem(
                    source="bhyt_price_comparison",
                    key=service_info.get("service_code", ""),
                    value=f"Cost {line.cost_vnd:,.0f} VND = {ratio:.1f}x BHYT ref {bhyt_price:,.0f} VND (>5x)",
                    weight=0.9,
                ))
            elif ratio > _PRICE_OUTLIER_RATIO:
                flags.append("price_outlier")
                evidence.append(EvidenceItem(
                    source="bhyt_price_comparison",
                    key=service_info.get("service_code", ""),
                    value=f"Cost {line.cost_vnd:,.0f} VND = {ratio:.1f}x BHYT ref {bhyt_price:,.0f} VND (>3x)",
                    weight=0.7,
                ))

        # 2. Duplicate services within the same claim
        normalized = _strip_diacritics(line.service_name_raw)
        count = sum(
            1 for other in all_lines
            if _strip_diacritics(other.service_name_raw) == normalized
        )
        if count > 1:
            flags.append("duplicate_service")
            evidence.append(EvidenceItem(
                source="duplicate_detection",
                key=line.service_name_raw,
                value=f"Service appears {count}x in the same claim",
                weight=0.6,
            ))

        # 3. Quantity anomaly
        if line.quantity > _HIGH_QUANTITY_THRESHOLD:
            flags.append("high_quantity")
            evidence.append(EvidenceItem(
                source="quantity_check",
                key=line.service_name_raw,
                value=f"Quantity={line.quantity} exceeds threshold {_HIGH_QUANTITY_THRESHOLD}",
                weight=0.5,
            ))

        if flags:
            return AgentVerdict(
                agent_name=self.agent_name,
                decision="review",
                confidence=0.70,
                evidence=evidence,
                flags=flags,
                reasoning_vi=f"Phat hien bat thuong: {', '.join(flags)}",
            )

        return AgentVerdict(
            agent_name=self.agent_name,
            decision="approve",
            confidence=0.90,
            evidence=[],
            flags=[],
            reasoning_vi="Khong phat hien bat thuong ve gia, trung lap, hoac so luong.",
        )
