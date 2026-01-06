from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional


def _get(d: Dict[str, Any], key: str, default: Any = "") -> Any:
    return d.get(key, default) if isinstance(d, dict) else default


def _as_list(value: Any) -> list:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [str(value)]


def _fmt_list(items: Iterable[Any]) -> str:
    lines = []
    for item in items:
        lines.append(f"- {item}")
    return "\n".join(lines) if lines else "- (없음)"


def _fmt_grouped_list(group: Dict[str, Any]) -> str:
    if not isinstance(group, dict) or not group:
        return "- (없음)"
    lines = []
    for key, value in group.items():
        lines.append(f"### {key}")
        lines.append(_fmt_list(_as_list(value)))
        lines.append("")
    return "\n".join(lines).strip()


def _fmt_score_table(scores: Dict[str, Any]) -> str:
    if not isinstance(scores, dict) or not scores:
        return "(없음)"
    lines = ["| 항목 | 점수 |", "|---|---|"]
    for key, value in scores.items():
        lines.append(f"| {key} | {value} |")
    return "\n".join(lines)


def render_report(
    file_name: str,
    step1: Dict[str, Any],
    step2: Optional[Dict[str, Any]],
    final_scores: Dict[str, Any],
    meeting_decision: str,
) -> str:
    one_line = _get(step1, "one_line_summary", "")
    logic_score = _get(step1, "logic_score", "")
    pass_gate = _get(step1, "pass_gate", "")
    step1_item_scores = _get(step1, "item_scores", {})
    step1_strengths = _get(step1, "strengths", {})
    step1_weaknesses = _get(step1, "weaknesses", {})
    step1_red_flags = _get(step1, "red_flags", [])

    stage_score = _get(step2, "stage_score", "") if step2 else ""
    industry_score = _get(step2, "industry_score", "") if step2 else ""
    bm_score = _get(step2, "bm_score", "") if step2 else ""
    axis_comments = _get(step2, "axis_comments", {}) if step2 else {}
    validation_questions = _get(step2, "validation_questions", {}) if step2 else {}
    now = datetime.now(tz=timezone.utc).isoformat()
    lines = [
        f"# IR Evaluation Report - {file_name}",
        "",
        f"Generated: {now} UTC",
        "",
        "## Summary",
        f"- One-line: {one_line}",
        f"- Step1 logic_score: {logic_score} / pass_gate: {pass_gate}",
        f"- Final score (conservative/neutral/optimistic): {final_scores}",
        f"- Meeting decision: {meeting_decision}",
        "",
        "## Step1 Item Scores",
        _fmt_score_table(step1_item_scores),
        "",
        "## Step1 Strengths (Investor View)",
        _fmt_grouped_list(step1_strengths),
        "",
        "## Step1 Weaknesses (Investor View)",
        _fmt_grouped_list(step1_weaknesses),
        "",
        "## Step1 Red Flags",
        _fmt_list(_as_list(step1_red_flags)),
    ]

    if step2:
        lines.extend(
            [
                "",
                "## Step2 Axis Scores",
                f"- stage/industry/BM: {stage_score} / {industry_score} / {bm_score}",
                "",
                "## Axis Comments",
                _fmt_grouped_list(axis_comments),
                "",
                "## Validation Questions",
                _fmt_grouped_list(validation_questions),
            ]
        )
    else:
        lines.extend(["", "## Step2", "Skipped due to pass_gate=false."])

    return "\n".join(lines)
