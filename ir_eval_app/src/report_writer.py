from __future__ import annotations

from datetime import datetime, timedelta, timezone
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


def _fmt_item_evaluations(items: Dict[str, Any]) -> str:
    if not isinstance(items, dict) or not items:
        return "(없음)"
    lines = ["| 항목 | 점수 | 평가 | 피드백 |", "|---|---|---|---|"]
    for key, value in items.items():
        if isinstance(value, dict):
            score = value.get("score", "")
            comment = value.get("comment", "")
            feedback = value.get("feedback", "")
        else:
            score = ""
            comment = ""
            feedback = ""
        lines.append(f"| {key} | {score} | {comment} | {feedback} |")
    return "\n".join(lines)


def _extract_company_name(file_name: str, step1: Dict[str, Any]) -> str:
    if step1.get("company_name"):
        return str(step1.get("company_name"))
    return "기업명 미상"


def render_report(
    file_name: str,
    step1: Dict[str, Any],
    step2: Optional[Dict[str, Any]],
    perspective_scores: Dict[str, Any],
    recommendations: Dict[str, Any],
    final_verdict: str,
) -> str:
    company_name = _extract_company_name(file_name, step1)
    one_line = _get(step1, "one_line_summary", "")
    overall_summary = _get(step1, "overall_summary", "")
    logic_score = _get(step1, "logic_score", "")
    pass_gate = _get(step1, "pass_gate", "")
    item_evaluations = _get(step1, "item_evaluations", {})
    strengths = _get(step1, "strengths", {})
    weaknesses = _get(step1, "weaknesses", {})
    red_flags = _get(step1, "red_flags", [])

    stage_score = _get(step2, "stage_score", "") if step2 else ""
    industry_score = _get(step2, "industry_score", "") if step2 else ""
    bm_score = _get(step2, "bm_score", "") if step2 else ""
    axis_comments = _get(step2, "axis_comments", {}) if step2 else {}
    validation_questions = _get(step2, "validation_questions", {}) if step2 else {}

    kst = timezone(timedelta(hours=9))
    now = datetime.now(tz=kst).strftime("%y.%m.%d")

    lines = [
        f"# {company_name} 분석 결과 {now}",
        "",
        f"기업설명: {one_line}",
        "",
        "## 평가 점수",
        "| 관점 | 점수 | 추천여부 |",
        "|---|---|---|",
        f"| Critical | {perspective_scores.get('critical', '')} | {recommendations.get('critical', '')} |",
        f"| Neutral | {perspective_scores.get('neutral', '')} | {recommendations.get('neutral', '')} |",
        f"| Positive | {perspective_scores.get('positive', '')} | {recommendations.get('positive', '')} |",
        "",
        f"- Step1 logic_score: {logic_score} / pass_gate: {pass_gate}",
        f"- Step2 axis score (stage/industry/BM): {stage_score} / {industry_score} / {bm_score}",
        "",
        "## 종합 평가",
        overall_summary or "(없음)",
        "",
        "## 항목별 평가",
        _fmt_item_evaluations(item_evaluations),
        "",
        "## 핵심 강점",
        _fmt_grouped_list(strengths),
        "",
        "## 핵심 취약점",
        _fmt_grouped_list(weaknesses),
        "",
        "## Red Flags",
        _fmt_list(_as_list(red_flags)),
        "",
        "## 산업/단계/BM 코멘트",
        _fmt_grouped_list(axis_comments),
        "",
        "## 검증 질문",
        _fmt_grouped_list(validation_questions),
        "",
        "## Final Verdict",
        final_verdict or "(없음)",
    ]

    return "\n".join(lines)
