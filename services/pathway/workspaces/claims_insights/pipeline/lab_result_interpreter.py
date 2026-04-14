from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


POSITIVE_RESULT_FLAGS = {"positive"}
NEGATIVE_RESULT_FLAGS = {"negative"}
NORMAL_RESULT_FLAGS = {"normal"}
ABNORMAL_RESULT_FLAGS = {"abnormal", "abnormal_high", "abnormal_low"}
ABNORMALITY_FLAGS = {"high", "low", "out_of_range"}


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


@dataclass(frozen=True)
class LabResultSignalSummary:
    observed: bool
    observed_count: int
    positive_count: int
    abnormal_count: int
    narrative_count: int
    negative_count: int
    normal_count: int
    has_positive_signal: bool
    has_negative_signal: bool
    has_abnormal_signal: bool
    has_conflicting_signals: bool
    supported: bool
    support_level: str


def classify_lab_result_signal(observation: dict[str, Any]) -> dict[str, Any]:
    result_flag = as_text(observation.get("result_flag")).lower()
    polarity = as_text(observation.get("polarity")).lower()
    abnormality = as_text(observation.get("abnormality")).lower()

    is_positive = result_flag in POSITIVE_RESULT_FLAGS or polarity == "positive"
    is_negative = result_flag in NEGATIVE_RESULT_FLAGS or polarity == "negative"
    is_normal = result_flag in NORMAL_RESULT_FLAGS or abnormality == "within_range"
    is_abnormal = result_flag in ABNORMAL_RESULT_FLAGS or abnormality in ABNORMALITY_FLAGS
    is_narrative = result_flag == "narrative"

    return {
        "result_flag": result_flag,
        "polarity": polarity,
        "abnormality": abnormality,
        "is_positive": is_positive,
        "is_negative": is_negative,
        "is_normal": is_normal,
        "is_abnormal": is_abnormal,
        "is_narrative": is_narrative,
    }


def summarize_lab_result_signals(observations: Iterable[dict[str, Any]]) -> LabResultSignalSummary:
    rows = list(observations)
    if not rows:
        return LabResultSignalSummary(
            observed=False,
            observed_count=0,
            positive_count=0,
            abnormal_count=0,
            narrative_count=0,
            negative_count=0,
            normal_count=0,
            has_positive_signal=False,
            has_negative_signal=False,
            has_abnormal_signal=False,
            has_conflicting_signals=False,
            supported=False,
            support_level="none",
        )

    positive_count = 0
    abnormal_count = 0
    narrative_count = 0
    negative_count = 0
    normal_count = 0

    for row in rows:
        signal = classify_lab_result_signal(row)
        positive_count += int(signal["is_positive"])
        abnormal_count += int(signal["is_abnormal"])
        narrative_count += int(signal["is_narrative"])
        negative_count += int(signal["is_negative"])
        normal_count += int(signal["is_normal"])

    has_positive_signal = positive_count > 0
    has_negative_signal = negative_count > 0
    has_abnormal_signal = abnormal_count > 0
    has_conflicting_signals = has_positive_signal and has_negative_signal

    if positive_count > 0 or abnormal_count >= 2:
        support_level = "strong"
        supported = True
    elif abnormal_count == 1 or narrative_count > 0:
        support_level = "moderate"
        supported = True
    else:
        support_level = "observed"
        supported = False

    return LabResultSignalSummary(
        observed=True,
        observed_count=len(rows),
        positive_count=positive_count,
        abnormal_count=abnormal_count,
        narrative_count=narrative_count,
        negative_count=negative_count,
        normal_count=normal_count,
        has_positive_signal=has_positive_signal,
        has_negative_signal=has_negative_signal,
        has_abnormal_signal=has_abnormal_signal,
        has_conflicting_signals=has_conflicting_signals,
        supported=supported,
        support_level=support_level,
    )
