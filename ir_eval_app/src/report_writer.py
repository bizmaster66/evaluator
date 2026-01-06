from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional


def _get(d: Dict[str, Any], key: str, default: Any = "") -> Any:
    return d.get(key, default) if isinstance(d, dict) else default


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
    def fmt_json(value: Any) -> str:
        import json

        return json.dumps(value, ensure_ascii=True, indent=2)

    now = datetime.now(tz=datetime.UTC).isoformat()
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
        "```json",
        fmt_json(step1_item_scores),
        "```",
        "",
        "## Step1 Strengths (Investor View)",
        "```json",
        fmt_json(step1_strengths),
        "```",
        "",
        "## Step1 Weaknesses (Investor View)",
        "```json",
        fmt_json(step1_weaknesses),
        "```",
        "",
        "## Step1 Red Flags",
        "```json",
        fmt_json(step1_red_flags),
        "```",
    ]

    if step2:
        lines.extend(
            [
                "",
                "## Step2 Axis Scores",
                f"- stage/industry/BM: {stage_score} / {industry_score} / {bm_score}",
                "",
                "## Axis Comments",
                "```json",
                fmt_json(axis_comments),
                "```",
                "",
                "## Validation Questions",
                "```json",
                fmt_json(validation_questions),
                "```",
            ]
        )
    else:
        lines.extend(["", "## Step2", "Skipped due to pass_gate=false."])

    return "\n".join(lines)
