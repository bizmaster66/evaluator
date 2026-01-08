from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional


def _get(d: Dict[str, Any], key: str, default: Any = "") -> Any:
    return d.get(key, default) if isinstance(d, dict) else default


def _as_list(value: Any) -> list:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [str(value)]


def _fmt_list(items: list) -> str:
    if not items:
        return "(없음)"
    return "\n".join([f"- {str(x)}" for x in items])


def _fmt_grouped_list(group: Any) -> str:
    # supports dict[str, list[str]] and dict[str, str]
    if not isinstance(group, dict) or not group:
        return "(없음)"
    lines = []
    for key, value in group.items():
        lines.append(f"### {key}")
        lines.append(_fmt_list(_as_list(value)))
        lines.append("")
    return "\n".join(lines).strip()


def _fmt_item_evaluations(items: Dict[str, Any]) -> str:
    if not isinstance(items, dict) or not items:
        return "(없음)"
    lines = ["| 항목 | 점수 | 평가 | 피드백 |", "|---|---|---|---|"]
    for key, value in items.items():
        if isinstance(value, dict):
            score = value.get("score", "")
            comment = (value.get("comment", "") or "").replace("\n", " ")
            feedback = (value.get("feedback", "") or "").replace("\n", " ")
        else:
            score = ""
            comment = ""
            feedback = ""
        lines.append(f"| {key} | {score} | {comment} | {feedback} |")
    return "\n".join(lines)


def _extract_company_name(file_name: str, step1: Dict[str, Any]) -> str:
    company_name = _get(step1, "company_name", "")
    if company_name:
        return str(company_name)
    # fallback: filename
    return file_name.rsplit(".", 1)[0]


def render_report(
    file_name: str,
    step1: Dict[str, Any],
    step2: Optional[Dict[str, Any]],
    perspective_scores: Dict[str, Any],
    recommendations: Dict[str, Any],
    final_verdict: str,
) -> str:
    """Render markdown report.

    This project now uses a *filtering* evaluation:
    - final_verdict: READ NOW / WATCH / DROP
    - logic_score: 0~92 (93+ is disallowed)
    - strengths: Evidence bullets
    - weaknesses: Gap bullets
    - red_flags: Risk bullets

    We keep backward-compatible sections so the existing UI/exports remain stable.
    """
    company_name = _extract_company_name(file_name, step1)
    one_line = _get(step1, "one_line_summary", "")
    overall_summary = _get(step1, "overall_summary", "")
    logic_score = _get(step1, "logic_score", "")

    # New fields (may be absent in older cache entries)
    step1_final = _get(step1, "final_verdict", "") or final_verdict
    exception_tag = _get(step1, "exception_tag", "")
    recommendation_message = _get(step1, "recommendation_message", "")

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

    # Compose report
    lines = [
        f"# {company_name} 분석 결과 {now}",
        "",
        f"기업설명: {one_line}",
        "",
        "## 필터링 결과",
        f"- 분류: {step1_final or '(없음)'}",
        f"- 종합 점수: {logic_score} / 100 (상한 92)",
        f"- 추천 메시지: {recommendation_message or '(없음)'}",
        f"- 예외 태그: {exception_tag or '(없음)'}",
        "",
        "## 평가 점수",
        "| 관점 | 점수 | 추천여부 |",
        "|---|---|---|",
        f"| Critical | {perspective_scores.get('critical', '')} | {recommendations.get('critical', '')} |",
        f"| Neutral | {perspective_scores.get('neutral', '')} | {recommendations.get('neutral', '')} |",
        f"| Positive | {perspective_scores.get('positive', '')} | {recommendations.get('positive', '')} |",
        "",
        "## 종합 요약",
        overall_summary or "(없음)",
        "",
        "## 근거 요약",
        "### Evidence (증명된 요소)",
        _fmt_grouped_list(strengths),
        "",
        "### Gap (정보 공백/미기재)",
        _fmt_grouped_list(weaknesses),
        "",
        "### Risk (구조적/치명 리스크 신호)",
        _fmt_list(_as_list(red_flags)),
        "",
        "## 항목별 평가 (호환용)",
        _fmt_item_evaluations(item_evaluations),
        "",
        "## 산업/단계/BM 코멘트 (선택)",
        _fmt_grouped_list(axis_comments),
        "",
        "## 검증 질문 (선택)",
        _fmt_grouped_list(validation_questions),
        "",
        "## Final Verdict",
        step1_final or "(없음)",
        "",
        "## Debug",
        f"- Step1 logic_score: {logic_score}",
        f"- Step2 axis score (stage/industry/BM): {stage_score} / {industry_score} / {bm_score}",
    ]

    return "\n".join(lines)
