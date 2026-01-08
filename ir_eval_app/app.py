from __future__ import annotations

import concurrent.futures
import hashlib
import json
import re
import threading
import time
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional

import openpyxl
import streamlit as st
from dateutil import tz

from src.config import MODEL_NAME, hash_prompt, md5_text, to_json
from src.evaluator import Evaluator
from src.report_writer import render_report

SCOPES = []

STEP1_SCHEMA_HINT = {
    "company_name": "string",
    "one_line_summary": "string",
    "overall_summary": "string (종합 평가 요약)",
    # ✅ 필터링 스코어: 0-92 (93점 이상 금지)
    "logic_score": "number 0-100 (단, 93점 이상 금지. 최대 92)",
    # ✅ 기존 필드 유지 (내부 로직/호환성)
    "pass_gate": "boolean (logic_score >= 70 -> WATCH 이상이면 True, 80 이상이면 READ NOW)",
    # ✅ 기존 필드 유지
    "perspective_scores": {
        "critical": "number 0-100",
        "neutral": "number 0-100",
        "positive": "number 0-100",
    },
    # ✅ 기존 필드 유지 (report_writer 등 호환성 대비)
    "item_evaluations": {
        "문제정의": {"score": "number 0-10", "comment": "string", "feedback": "string"},
        "솔루션&제품": {"score": "number 0-10", "comment": "string", "feedback": "string"},
        "시장규모&분석": {"score": "number 0-10", "comment": "string", "feedback": "string"},
        "비즈니스모델": {"score": "number 0-10", "comment": "string", "feedback": "string"},
        "경쟁분석": {"score": "number 0-10", "comment": "string", "feedback": "string"},
        "성장전략": {"score": "number 0-10", "comment": "string", "feedback": "string"},
        "주요 인력&팀": {"score": "number 0-10", "comment": "string", "feedback": "string"},
        "재무계획": {"score": "number 0-10", "comment": "string", "feedback": "string"},
    },
    "item_scores": {"market": "number 0-10", "team": "number 0-10", "product": "number 0-10"},
    # ✅ 구조화 근거를 기존 strengths/weaknesses/red_flags로 매핑할 수 있도록 유지
    "strengths": {"market": "list[str]", "team": "list[str]", "product": "list[str]"},
    "weaknesses": {"market": "list[str]", "team": "list[str]", "product": "list[str]"},
    "red_flags": "list[str]",
    # ✅ NEW: 최종 분류(READ NOW/WATCH/DROP) 및 예외 태그(LOW_SCORE_BUT_READ)
    "final_verdict": "string (READ NOW/WATCH/DROP)",
    "exception_tag": "string (LOW_SCORE_BUT_READ or empty)",
    "recommendation_message": "string (READ NOW/WATCH/DROP에 따른 메시지)",
    "cost_estimate": {"llm_calls": "number", "tokens": "number", "usd": "number"},
}

STEP2_SCHEMA_HINT = {
    "stage_label": "string (Seed/Pre-Seed/Series A/Series B+/Unknown)",
    "industry_label": "string (SaaS/Commerce/Bio-Healthcare/DeepTech/Other)",
    "stage_score": "number 0-10",
    "industry_score": "number 0-10",
    "bm_score": "number 0-10",
    "axis_comments": {"stage": "string", "industry": "string", "bm": "string"},
    "validation_questions": {"stage": "list[str]", "industry": "list[str]", "bm": "list[str]"},
    "cost_estimate": {"llm_calls": "number", "tokens": "number", "usd": "number"},
}

SHEET_COLUMNS = [
    "timestamp(KST)",
    "file_name",
    "company_name",
    "company_description",
    "score_critical",
    "score_neutral",
    "score_positive",
    "recommendation_critical",
    "recommendation_neutral",
    "recommendation_positive",
    "overall_summary",
    "item_evaluations_json",
    "strengths_json",
    "weaknesses_json",
    "red_flags_json",
    "axis_scores_json",
    "axis_comments_json",
    "validation_questions_json",
    "final_verdict",
]

STATUS_PENDING = "대기"
STATUS_RUNNING = "진행중"
STATUS_SKIPPED = "스킵"
STATUS_DONE = "완료"
STATUS_FAILED = "실패"

ITEM_KEYS = [
    "문제정의",
    "솔루션&제품",
    "시장규모&분석",
    "비즈니스모델",
    "경쟁분석",
    "성장전략",
    "주요 인력&팀",
    "재무계획",
]

# ✅ PROMPT_APPENDIX는 “기존 Step1/Step2 평가 지시” 대신,
#    “Step1 결과(JSON)에 필터링 분류/구조화 근거를 정확히 채우도록”만 보조한다.
PROMPT_APPENDIX = (
    "추가 지시사항:\n"
    "1) Step1 JSON은 반드시 스키마 힌트에 맞춰 출력한다.\n"
    "2) final_verdict는 반드시 READ NOW / WATCH / DROP 중 하나로 출력한다.\n"
    "3) logic_score는 0~92 범위로 출력하고, 93점 이상은 절대 금지한다.\n"
    "4) 분류 기본 규칙:\n"
    "   - READ NOW: 80~92\n"
    "   - WATCH: 70~79\n"
    "   - DROP: 0~69\n"
    "5) 단, 예외적으로 점수가 낮아도 읽어야 할 이유가 명확하면:\n"
    "   - final_verdict를 WATCH 또는 READ NOW로 상향할 수 있다.\n"
    "   - 이 경우 exception_tag를 'LOW_SCORE_BUT_READ'로 설정한다.\n"
    "   - 상향 근거는 strengths(=Evidence)에서 특출난 준비 요소를 명확히 지목해야 한다.\n"
    "6) recommendation_message는 final_verdict에 따라 반드시 다음 문구 중 하나로 출력한다:\n"
    "   - READ NOW: '지금 읽을 가치가 있음'\n"
    "   - WATCH: '추가 검토를 고려할 수 있음'\n"
    "   - DROP: '' (빈 문자열)\n"
    "7) 구조화 근거 작성 규칙:\n"
    "   - strengths.* 에 Evidence(증명된 요소) 불릿을 총 3~6개 채운다.\n"
    "   - weaknesses.* 에 Gap(정보 공백/미기재) 불릿을 총 3~6개 채운다.\n"
    "   - red_flags 에 Risk(구조적/치명 리스크 신호) 불릿을 3~6개 채운다.\n"
    "   - 단, Risk는 추정이 아니라 입력 텍스트에 근거한 신호만 쓴다.\n"
    "8) overall_summary에는 반드시 아래 구성으로 작성한다(3~7줄):\n"
    "   - 분류/점수/예외태그(해당 시)\n"
    "   - 왜 그렇게 분류했는지의 핵심 근거 요약\n"
    "   - 투자 추천/성공 가능성/확장 가능성/전망 문장은 금지\n"
    "9) item_evaluations는 호환성을 위해 비워두지 말고, 각 항목 score=0~10과 comment/feedback을 간단히 채우되\n"
    "   - '가능하다/기대된다/성공' 같은 전망형 문장 금지\n"
    "   - 과도한 미사여구 금지\n"
)

# ✅ BASE_PROMPT를 “설명문 기반 2단계 필터링 평가 프롬프트”로 교체
#    (요약/축약 없이 그대로 삽입)
BASE_PROMPT = """당신은 벤처캐피탈 내부 심사역을 보조하는 ‘IR 필터링(선별) 심사역’이다.
입력으로 주어지는 텍스트는 “IR PDF를 사실 중심으로 변환한 설명문(Markdown)”이며,
당신의 목적은 투자 결정을 대신하는 것이 아니라, 사람이 시간을 들여 읽어야 할 IR인지 여부를
“READ NOW / WATCH / DROP”으로 분류하고, 그 근거를 구조화하여 제공하는 것이다.

이 평가는 ‘좋은 팀을 칭찬’하거나 ‘성공 가능성을 예측’하는 작업이 아니다.
당신은 아래 기준에 따라, “지금 읽어야 할 가치가 있는 IR인지”를 추천하는 역할만 수행한다.

────────────────────────────────────────────────────────────────────────
0) 입력 데이터 원칙 (Source-of-truth)
────────────────────────────────────────────────────────────────────────
- 입력(설명문)에 포함된 내용만 근거로 사용한다.
- 입력에 없는 내용은 생성/추정/보완하지 않는다.
- 외부 정보/시장 데이터 인용은 금지한다. (이 단계에서는 입력 텍스트만 사용)
- 문구가 모호하면 “불명확/근거 부족/미기재”로 처리한다.
- “회사/제품/서비스 고유명사”는 입력에 등장하는 표기를 그대로 사용한다.

────────────────────────────────────────────────────────────────────────
1) 평가 목표 (What you must output)
────────────────────────────────────────────────────────────────────────
당신의 출력은 아래를 반드시 포함한다.

[결론]
- 분류: READ NOW / WATCH / DROP
- 종합 점수: XX / 100  (단, 93점 이상은 부여하지 않는다. 최대 92점)
- 추천 메시지:
  - READ NOW: “지금 읽을 가치가 있음”
  - WATCH: “추가 검토를 고려할 수 있음”
  - DROP: “(표준 메시지 없이)”
- 예외 태그(선택): “LOW_SCORE_BUT_READ” (점수는 낮지만 읽어야 할 이유가 있을 때만)

[근거 요약]
- Evidence (증명된 요소)
- Gap (정보 공백)
- Risk (구조적 리스크)

[판단 근거 요약]
- 3~7줄로, 왜 그렇게 분류했는지 설명한다.
- 단, “투자 추천/매력도/성공 가능성” 판단은 하지 않는다.
- “~할 수 있다/기대된다/가능하다” 같은 가능성·전망 문장은 쓰지 않는다.
- “좋은 팀/우수한 팀/A급” 같은 정성적 칭찬은 쓰지 않는다.

────────────────────────────────────────────────────────────────────────
2) 분류 기준 (Classification policy)
────────────────────────────────────────────────────────────────────────
기본 분류는 점수에 의해 결정된다.

- READ NOW: 80 ~ 92점
- WATCH: 70 ~ 79점
- DROP: 0 ~ 69점
- 93점 이상은 부여 금지 (최대 92점)

단, 예외적으로 점수와 분류가 다를 수 있다.
이 예외는 “점수는 낮지만 특정 항목이 매우 강하게 준비되어 있어 사람이 시간을 들여 읽을 이유가 명확한 경우”에만 허용한다.

예외 규칙:
- 점수가 70점 미만이더라도, 아래 조건을 만족하면 분류를 WATCH 또는 READ NOW로 상향할 수 있다.
- 단, 이 경우 반드시 예외 태그 “LOW_SCORE_BUT_READ”를 [결론]에 추가한다.
- 상향의 근거는 Evidence 항목에서 “특출난 준비 요소”를 명확히 지목해야 한다.
- 상향은 ‘정보 부족’ 때문이 아니라 ‘강한 증거/검증’ 때문에만 허용한다.

허용되는 상향의 대표 조건(예시):
- 강한 실증/검증 데이터가 입력에 명확히 존재 (PoC/실제 운영 지표/반복 측정 지표 등)
- 유닛 이코노믹스의 핵심 수치가 구체적이며 논리적 연결이 깨지지 않음
- 시장/고객/문제-솔루션 정합성이 구체적이고 내부 논리 공백이 매우 적음
(※ 위는 예시이며, 반드시 입력에 명시된 근거로만 판단)

────────────────────────────────────────────────────────────────────────
3) 점수 산정 (Scoring rubric: B+C 혼합)
────────────────────────────────────────────────────────────────────────
종합 점수는 “IR 완성도/검증 수준(B)”과 “읽을 우선순위(C)”를 혼합한다.
단, 투자 매력도 점수가 아니다. “읽을 가치”의 객관화 지표다.

아래 5개 축을 각각 0~20점으로 평가하고, 합산하여 0~100을 만든 뒤, 최종 점수는 92를 상한으로 캡한다.

(1) Evidence Strength (0~20)
- 입력에서 확인되는 검증/실측/성과/지표의 구체성, 측정 방법의 명료성, 반복/추세 데이터 존재 여부

(2) Problem–Solution Clarity (0~20)
- 문제 정의의 구체성, 고객/상황의 명확성, 솔루션이 문제와 직접 연결되는지, 설명의 모호성 여부

(3) Business Model & Unit Economics Clarity (0~20)
- 수익 구조, 가격/과금 기준, 비용 구조, 핵심 지표(LTV/CAC 등)의 논리적 일관성
- 수치가 있는데 근거/정의가 없으면 감점

(4) Market & Customer Grounding (0~20)
- 타깃 고객/시장 범위가 구체적인지, 시장 정의가 비약적이지 않은지,
- 고객 획득/세그먼트/채널이 설명되는지

(5) Execution Readiness (0~20)
- 실행 계획/로드맵/조직/운영의 구체성, 현실적인 단계 설정,
- “누가/무엇을/언제/어떻게”가 입력에 드러나는 정도

각 축 점수는 근거가 있는 항목만 올릴 수 있다.
입력에 없는 정보는 “없음/미기재”로 처리하고 해당 축에서 감점한다.

최종 점수 계산:
- raw_score = (1)+(2)+(3)+(4)+(5)  (0~100)
- final_score = min(raw_score, 92)  (상한 92점)

────────────────────────────────────────────────────────────────────────
4) Evidence / Gap / Risk 작성 규칙 (구조화 근거)
────────────────────────────────────────────────────────────────────────
- Evidence: 입력에서 “검증된 사실/지표/성과/실험”을 중심으로 쓴다. (3~6개)
- Gap: 입력에서 “명시되지 않은 핵심 정보/정의/근거/수치/방법”을 쓴다. (3~6개)
- Risk: 입력에서 드러난 “구조적 리스크/치명 리스크/일관성 붕괴/규제·데이터 이슈 미기재” 등을 쓴다. (3~6개)
  - 단, Risk는 추정이 아니라 “입력에 근거한 리스크 신호”만 적는다.
  - 예: “개인정보/부정사용 방지에 대한 설명이 미기재됨”은 가능
  - 예: “개인정보 이슈로 큰일 난다” 같은 예측은 금지

────────────────────────────────────────────────────────────────────────
5) 금지 사항 (Hard bans)
────────────────────────────────────────────────────────────────────────
- 입력에 없는 내용 생성/추정/보완 금지
- 외부 데이터/시장 자료 인용 금지
- 투자 권유/추천/매력도 판단 금지
- 성공 가능성/확장 가능성/미래 전망 서술 금지
- 과도한 수식어/감정적 표현/칭찬형 표현 금지
- “가능하다/기대된다/열려 있다” 등 전망형 문장 금지

────────────────────────────────────────────────────────────────────────
이제 입력으로 제공되는 “IR 설명문(Markdown)”만을 근거로 Step1 JSON을 스키마에 맞춰 작성하라.
"""


def get_api_key() -> str:
    api_key = st.secrets.get("gemini", {}).get("api_key")
    if not api_key:
        raise RuntimeError("Missing gemini api key in Streamlit secrets")
    return api_key


def kst_now() -> str:
    kst = tz.gettz("Asia/Seoul")
    return datetime.now(tz=kst).strftime("%Y-%m-%d %H:%M:%S")


def cache_key_for(content: str, step1_hash: str, step2_hash: str) -> str:
    parts = [md5_text(content), step1_hash, step2_hash, MODEL_NAME]
    return hashlib.sha256("::".join(parts).encode("utf-8")).hexdigest()


DEFAULT_WEIGHTS = {
    "문제정의": 0.125,
    "솔루션&제품": 0.125,
    "시장규모&분석": 0.125,
    "비즈니스모델": 0.125,
    "경쟁분석": 0.125,
    "성장전략": 0.125,
    "주요 인력&팀": 0.125,
    "재무계획": 0.125,
}

STAGE_WEIGHTS = {
    "Seed": {
        "문제정의": 0.18,
        "솔루션&제품": 0.18,
        "시장규모&분석": 0.12,
        "비즈니스모델": 0.10,
        "경쟁분석": 0.08,
        "성장전략": 0.10,
        "주요 인력&팀": 0.16,
        "재무계획": 0.08,
    },
    "Pre-Seed": {
        "문제정의": 0.19,
        "솔루션&제품": 0.18,
        "시장규모&분석": 0.12,
        "비즈니스모델": 0.08,
        "경쟁분석": 0.08,
        "성장전략": 0.10,
        "주요 인력&팀": 0.17,
        "재무계획": 0.08,
    },
    "Series A": {
        "문제정의": 0.10,
        "솔루션&제품": 0.12,
        "시장규모&분석": 0.18,
        "비즈니스모델": 0.16,
        "경쟁분석": 0.10,
        "성장전략": 0.16,
        "주요 인력&팀": 0.10,
        "재무계획": 0.08,
    },
    "Series B+": {
        "문제정의": 0.08,
        "솔루션&제품": 0.10,
        "시장규모&분석": 0.14,
        "비즈니스모델": 0.20,
        "경쟁분석": 0.14,
        "성장전략": 0.16,
        "주요 인력&팀": 0.08,
        "재무계획": 0.10,
    },
}

INDUSTRY_WEIGHTS = {
    "SaaS": {
        "문제정의": 0.10,
        "솔루션&제품": 0.12,
        "시장규모&분석": 0.18,
        "비즈니스모델": 0.18,
        "경쟁분석": 0.14,
        "성장전략": 0.14,
        "주요 인력&팀": 0.08,
        "재무계획": 0.06,
    },
    "Commerce": {
        "문제정의": 0.10,
        "솔루션&제품": 0.10,
        "시장규모&분석": 0.18,
        "비즈니스모델": 0.20,
        "경쟁분석": 0.12,
        "성장전략": 0.16,
        "주요 인력&팀": 0.08,
        "재무계획": 0.06,
    },
    "Bio-Healthcare": {
        "문제정의": 0.16,
        "솔루션&제품": 0.18,
        "시장규모&분석": 0.12,
        "비즈니스모델": 0.10,
        "경쟁분석": 0.10,
        "성장전략": 0.10,
        "주요 인력&팀": 0.14,
        "재무계획": 0.10,
    },
    "DeepTech": {
        "문제정의": 0.14,
        "솔루션&제품": 0.20,
        "시장규모&분석": 0.12,
        "비즈니스모델": 0.10,
        "경쟁분석": 0.12,
        "성장전략": 0.10,
        "주요 인력&팀": 0.14,
        "재무계획": 0.08,
    },
}


def _normalize_weights(weights: Dict[str, float]) -> Dict[str, float]:
    total = sum(weights.values()) or 1.0
    return {k: v / total for k, v in weights.items()}


def _combine_weights(stage_label: str, industry_label: str) -> Dict[str, float]:
    stage_weights = STAGE_WEIGHTS.get(stage_label, DEFAULT_WEIGHTS)
    industry_weights = INDUSTRY_WEIGHTS.get(industry_label, DEFAULT_WEIGHTS)
    combined = {}
    for key in ITEM_KEYS:
        combined[key] = (DEFAULT_WEIGHTS[key] + stage_weights[key] + industry_weights[key]) / 3.0
    return _normalize_weights(combined)


def _weighted_item_score(step1: Dict[str, Any], step2: Optional[Dict[str, Any]]) -> float:
    items = step1.get("item_evaluations", {}) if isinstance(step1, dict) else {}
    stage_label = ""
    industry_label = ""
    if step2 and isinstance(step2, dict):
        stage_label = str(step2.get("stage_label", "") or "")
        industry_label = str(step2.get("industry_label", "") or "")
    weights = _combine_weights(stage_label, industry_label)
    total = 0.0
    for key in ITEM_KEYS:
        item = items.get(key, {})
        try:
            score = float(item.get("score", 0) or 0)
        except (TypeError, ValueError):
            score = 0.0
        total += score * weights[key]
    return max(0.0, min(10.0, total)) * 10.0


def compute_perspective_scores(step1: Dict[str, Any], step2: Optional[Dict[str, Any]]) -> Dict[str, int]:
    # ✅ 필터링 목적: logic_score(0~92)를 핵심 점수로 사용
    # 기존 UI/엑셀 구조를 유지하기 위해 critical/neutral/positive를 동일 점수로 세팅
    logic_score = float(step1.get("logic_score", 0) or 0)
    s = min(92, int(round(max(0, logic_score))))
    return {"critical": s, "neutral": s, "positive": s}


def recommendation_for(score: int) -> str:
    # ✅ 메시지 규칙(요청 반영)
    if score >= 80:
        return "지금 읽을 가치가 있음"
    if score >= 70:
        return "추가 검토를 고려할 수 있음"
    return ""


def derive_recommendations(scores: Dict[str, int]) -> Dict[str, str]:
    return {k: recommendation_for(v) for k, v in scores.items()}


def format_error_info(exc: Exception, file_name: str) -> Dict[str, str]:
    message = str(exc).replace("\n", " ")[:300]
    return {
        "type": exc.__class__.__name__,
        "message": message,
        "file_name": file_name,
    }


def evaluate_one(
    evaluator: Evaluator,
    content: str,
    file_name: str,
    step1_hash: str,
    step2_hash: str,
    force_rerun: bool,
    cache: Dict[str, Any],
) -> Dict[str, Any]:
    key = cache_key_for(content, step1_hash, step2_hash)
    if key in cache and not force_rerun:
        return {"status": STATUS_SKIPPED, "cache": cache[key], "file_name": file_name}

    step1_json = evaluator.evaluate_step1(
        content=content,
        prompt_step1=f"{BASE_PROMPT}\n\n{PROMPT_APPENDIX}",
        schema_hint_step1=to_json(STEP1_SCHEMA_HINT),
    )

    # ✅ 점수(0~92) 및 분류 우선 적용
    logic_score = float(step1_json.get("logic_score", 0) or 0)
    logic_score = min(92.0, max(0.0, logic_score))
    step1_json["logic_score"] = logic_score

    # ✅ 기본 분류 규칙 (단, 모델이 final_verdict를 명시하면 그것을 우선 신뢰)
    model_verdict = str(step1_json.get("final_verdict", "") or "").strip()
    if model_verdict in ("READ NOW", "WATCH", "DROP"):
        final_verdict = model_verdict
    else:
        if logic_score >= 80:
            final_verdict = "READ NOW"
        elif logic_score >= 70:
            final_verdict = "WATCH"
        else:
            final_verdict = "DROP"
        step1_json["final_verdict"] = final_verdict

    # ✅ pass_gate는 WATCH 이상(>=70)이면 True로 설정 (기존 로직 호환)
    step1_json["pass_gate"] = logic_score >= 70

    # Step2는 원칙적으로 필터링에는 불필요하나, 호환성을 위해 형태는 유지
    step2_json: Optional[Dict[str, Any]] = None
    # 기존: if step1_json.get("pass_gate", False):
    # 지금은 필터링 목적상 Step2를 돌리지 않는 것을 기본으로 한다.
    # (필요하면 추후 옵션으로 켤 수 있음)

    scores = compute_perspective_scores(step1_json, step2_json)
    recommendations = derive_recommendations(scores)

    # ✅ 화면/엑셀에 보일 verdict는 Step1의 final_verdict를 사용
    final_verdict = step1_json.get("final_verdict", final_verdict)

    report_md = render_report(
        file_name,
        step1_json,
        step2_json,
        scores,
        recommendations,
        final_verdict,
    )

    result_payload = {
        "file_name": file_name,
        "timestamp": kst_now(),
        "company_name": step1_json.get("company_name", ""),
        "company_description": step1_json.get("one_line_summary", ""),
        "scores": scores,
        "recommendations": recommendations,
        "final_verdict": final_verdict,
        "overall_summary": step1_json.get("overall_summary", ""),
        "item_evaluations": step1_json.get("item_evaluations", {}),
        "strengths": step1_json.get("strengths", {}),
        "weaknesses": step1_json.get("weaknesses", {}),
        "red_flags": step1_json.get("red_flags", []),
        "axis_scores": {
            "stage": step2_json.get("stage_score") if step2_json else "",
            "industry": step2_json.get("industry_score") if step2_json else "",
            "bm": step2_json.get("bm_score") if step2_json else "",
        },
        "axis_comments": step2_json.get("axis_comments") if step2_json else {},
        "validation_questions": step2_json.get("validation_questions") if step2_json else {},
        "step1_json": step1_json,
        "step2_json": step2_json,
    }

    cache_entry = {
        "file_name": file_name,
        "timestamp": kst_now(),
        "step1": step1_json,
        "step2": step2_json,
        "report_md": report_md,
        "result_json": result_payload,
        "perspective_scores": scores,
        "recommendations": recommendations,
        "final_verdict": final_verdict,
        "status": STATUS_DONE,
        "cache_key": key,
    }
    cache[key] = cache_entry
    return {"status": STATUS_DONE, "cache": cache_entry, "file_name": file_name}


def build_sheet_row(entry: Dict[str, Any]) -> Dict[str, Any]:
    step1 = entry.get("step1", {})
    scores = entry.get("perspective_scores", {})
    recommendations = entry.get("recommendations", {})
    step2 = entry.get("step2", {})
    return {
        "timestamp(KST)": entry.get("timestamp", kst_now()),
        "file_name": entry.get("file_name", ""),
        "company_name": step1.get("company_name", ""),
        "company_description": step1.get("one_line_summary", ""),
        "score_critical": scores.get("critical", ""),
        "score_neutral": scores.get("neutral", ""),
        "score_positive": scores.get("positive", ""),
        "recommendation_critical": recommendations.get("critical", ""),
        "recommendation_neutral": recommendations.get("neutral", ""),
        "recommendation_positive": recommendations.get("positive", ""),
        "overall_summary": step1.get("overall_summary", ""),
        "item_evaluations_json": json.dumps(step1.get("item_evaluations", {}), ensure_ascii=True),
        "strengths_json": json.dumps(step1.get("strengths", {}), ensure_ascii=True),
        "weaknesses_json": json.dumps(step1.get("weaknesses", {}), ensure_ascii=True),
        "red_flags_json": json.dumps(step1.get("red_flags", []), ensure_ascii=True),
        "axis_scores_json": json.dumps(
            {
                "stage": step2.get("stage_score", "") if isinstance(step2, dict) else "",
                "industry": step2.get("industry_score", "") if isinstance(step2, dict) else "",
                "bm": step2.get("bm_score", "") if isinstance(step2, dict) else "",
            },
            ensure_ascii=True,
        ),
        "axis_comments_json": json.dumps(step2.get("axis_comments", {}) if isinstance(step2, dict) else {}, ensure_ascii=True),
        "validation_questions_json": json.dumps(
            step2.get("validation_questions", {}) if isinstance(step2, dict) else {}, ensure_ascii=True
        ),
        "final_verdict": entry.get("final_verdict", ""),
    }


def cache_to_excel_bytes(cache: Dict[str, Any]) -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "IR_EVAL"
    ws.append(SHEET_COLUMNS)
    for entry in cache.values():
        row = build_sheet_row(entry)
        ws.append([row.get(col, "") for col in SHEET_COLUMNS])
    buffer = BytesIO()
    wb.save(buffer)
    return buffer.getvalue()


def excel_filename() -> str:
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M")
    return f"ir_eval_{stamp}.xlsx"


def status_badge(status: str) -> str:
    mapping = {
        STATUS_DONE: "✅완료",
        STATUS_PENDING: "🕒대기",
        STATUS_FAILED: "⚠️실패",
        STATUS_RUNNING: "🔄진행중",
        STATUS_SKIPPED: "✅완료",
    }
    return mapping.get(status, status or "-")


def render_preview_panel(entry: Optional[Dict[str, Any]]) -> None:
    st.subheader("미리보기")
    if not entry:
        st.info("선택된 리포트가 없습니다.")
        return

    step1 = entry.get("step1", {})
    scores = entry.get("perspective_scores", {})
    company_name = step1.get("company_name") or "기업명 미상"
    st.markdown(
        f"""
        <div class="preview-card">
          <div class="preview-title">리포트 제목 : {company_name}</div>
          <div class="preview-sub">Critical : {scores.get('critical','')} &nbsp;&nbsp;
          Neutral : {scores.get('neutral','')} &nbsp;&nbsp;
          Positive : {scores.get('positive','')}</div>
          <div style="margin-top:0.6rem;">{step1.get("one_line_summary", "")}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""
        <div class="preview-card">
          <div class="preview-title">Title : 종합 평가</div>
          <div>{step1.get("overall_summary", "(없음)")}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    item_evaluations = step1.get("item_evaluations", {})
    if not item_evaluations:
        st.info("항목별 평가가 없습니다.")
        return

    st.markdown("### 항목별 평가")
    for i in range(0, len(ITEM_KEYS), 2):
        cols = st.columns(2)
        for j, key in enumerate(ITEM_KEYS[i : i + 2]):
            value = item_evaluations.get(key, {})
            comment = value.get("comment", "")
            feedback = value.get("feedback", "")
            cols[j].markdown(
                f"""
                <div class="preview-card">
                  <div class="preview-title">Title : {key}</div>
                  <div>{comment or "(코멘트 없음)"}</div>
                  <div style="margin-top:0.5rem;">{feedback or "(피드백 없음)"}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            text = f"{comment} {feedback}".strip()
            sentences = [s for s in re.split(r"[.!?]\s+", text) if s.strip()]
            if len(sentences) < 6:
                cols[j].caption("권장 분량: comment 5~8문장, feedback 4~5문장")


def init_session_state() -> None:
    st.session_state.setdefault("files", [])
    st.session_state.setdefault("cache", {})
    st.session_state.setdefault("status_map", {})
    st.session_state.setdefault("selected_file_ids", [])
    st.session_state.setdefault("selected_file_name", "")
    st.session_state.setdefault("page", 1)


def main() -> None:
    st.set_page_config(page_title="IR Evaluator", layout="wide")
    st.markdown(
        """
        <style>
        .block-container { padding-top: 1.2rem; padding-bottom: 1.6rem; }
        .table-header { font-weight: 700; color: #2b2b2b; font-size: 0.95rem; }
        .muted { color: #6b7280; font-size: 0.85rem; }
        .compact .stButton>button { padding: 0.25rem 0.6rem; font-size: 0.85rem; }
        .compact .stCheckbox { padding-top: 0.2rem; }
        .compact .stTextInput>div>div>input { height: 2rem; }
        .compact .stFileUploader { padding-bottom: 0.2rem; }
        .compact .stMarkdown { margin-bottom: 0.15rem; }
        .row-compact { font-size: 0.88rem; }
        .preview-card {
            border: 1px solid #e5e7eb;
            border-radius: 10px;
            padding: 0.8rem 0.9rem;
            background: #fafafa;
            margin-bottom: 0.7rem;
        }
        .preview-title { font-weight: 700; margin-bottom: 0.4rem; }
        .preview-sub { color: #6b7280; font-size: 0.85rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("IR 분석 & 평가")

    try:
        api_key = get_api_key()
    except RuntimeError as exc:
        st.error(str(exc))
        st.stop()

    init_session_state()

    st.markdown("<div class='compact'>", unsafe_allow_html=True)
    top_cols = st.columns([5, 1, 1, 1, 1], gap="small")
    with top_cols[0]:
        uploaded_files = st.file_uploader(
            "IR Markdown 업로드 (.md)",
            type=["md"],
            accept_multiple_files=True,
            label_visibility="visible",
        )
    with top_cols[1]:
        scan_clicked = st.button("문서 스캔", use_container_width=True)
    with top_cols[2]:
        force_rerun = st.checkbox("캐시 무시(재평가)", value=False)
    with top_cols[3]:
        refresh_clicked = st.button("캐시 새로고침", use_container_width=True)
    with top_cols[4]:
        delete_cache_clicked = st.button("캐시 삭제", use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

    if refresh_clicked:
        st.session_state["status_map"] = st.session_state.get("status_map", {})

    if delete_cache_clicked:
        st.session_state["cache"] = {}
        st.session_state["status_map"] = {}

    if scan_clicked and uploaded_files:
        st.session_state["files"] = uploaded_files
        st.session_state["status_map"] = {f.name: STATUS_PENDING for f in uploaded_files}

    files = st.session_state.get("files", [])
    if not files:
        st.info("파일을 업로드하면 .md 파일 목록이 나타납니다.")
        return

    table_header = st.columns([3, 1], gap="small")
    table_header[0].subheader("파일 목록 & IR List")
    if st.session_state.get("cache"):
        excel_bytes = cache_to_excel_bytes(st.session_state["cache"])
        table_header[1].download_button(
            label="엑셀 다운로드",
            data=excel_bytes,
            file_name=excel_filename(),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    else:
        table_header[1].button("엑셀 다운로드", disabled=True, use_container_width=True)

    st.markdown("<div class='compact'>", unsafe_allow_html=True)
    search_term = st.text_input("검색(파일명/기업명)", value="", placeholder="파일명 또는 기업명")
    st.markdown("</div>", unsafe_allow_html=True)

    header_cols = st.columns([3, 1, 0.8, 1.2, 1, 1, 1, 1, 1], gap="small")
    header_cols[0].markdown("<div class='table-header'>파일명</div>", unsafe_allow_html=True)
    header_cols[1].markdown("<div class='table-header'>진행</div>", unsafe_allow_html=True)
    header_cols[2].markdown("<div class='table-header'>선택</div>", unsafe_allow_html=True)
    header_cols[3].markdown("<div class='table-header'>기업명</div>", unsafe_allow_html=True)
    header_cols[4].markdown("<div class='table-header'>critical</div>", unsafe_allow_html=True)
    header_cols[5].markdown("<div class='table-header'>neutral</div>", unsafe_allow_html=True)
    header_cols[6].markdown("<div class='table-header'>positive</div>", unsafe_allow_html=True)
    header_cols[7].markdown("<div class='table-header'>미리보기</div>", unsafe_allow_html=True)
    header_cols[8].markdown("<div class='table-header'>파일열기</div>", unsafe_allow_html=True)

    selected_ids = set(st.session_state.get("selected_file_ids", []))
    cache = st.session_state.get("cache", {})
    cache_by_name = {entry.get("file_name", ""): entry for entry in cache.values()}
    status_map = st.session_state.get("status_map", {})
    for f in files:
        entry = cache_by_name.get(f.name)
        if not entry:
            continue
        cached_status = entry.get("status", STATUS_DONE)
        if status_map.get(f.name) != cached_status:
            status_map[f.name] = cached_status
    st.session_state["status_map"] = status_map

    filtered_files = []
    for f in files:
        entry = cache_by_name.get(f.name)
        company_name = entry.get("step1", {}).get("company_name", "") if entry else ""
        if search_term:
            term = search_term.strip().lower()
            if term not in f.name.lower() and term not in company_name.lower():
                continue
        filtered_files.append(f)

    page_size = 10
    total_pages = max(1, (len(filtered_files) + page_size - 1) // page_size)
    page = min(st.session_state.get("page", 1), total_pages)
    pager_cols = st.columns([1, 1, 2, 1, 1], gap="small")
    if pager_cols[0].button("이전", use_container_width=True):
        page = max(1, page - 1)
    pager_cols[2].markdown(f"<div class='muted'>페이지 {page}/{total_pages}</div>", unsafe_allow_html=True)
    if pager_cols[4].button("다음", use_container_width=True):
        page = min(total_pages, page + 1)
    st.session_state["page"] = page

    start = (page - 1) * page_size
    end = start + page_size
    for f in filtered_files[start:end]:
        entry = cache_by_name.get(f.name)
        company_name = entry.get("step1", {}).get("company_name", "") if entry else ""
        scores = entry.get("perspective_scores", {}) if entry else {}

        row = st.columns([3, 1, 0.8, 1.2, 1, 1, 1, 1, 1], gap="small")
        row[0].markdown(f"<div class='row-compact'>{f.name}</div>", unsafe_allow_html=True)
        row[1].markdown(
            f"<div class='row-compact'>{status_badge(st.session_state['status_map'].get(f.name, STATUS_PENDING))}</div>",
            unsafe_allow_html=True,
        )
        checked = row[2].checkbox(
            "",
            value=f.name in selected_ids,
            key=f"select_{f.name}",
        )
        if checked:
            selected_ids.add(f.name)
        else:
            selected_ids.discard(f.name)
        row[3].markdown(f"<div class='row-compact'>{company_name}</div>", unsafe_allow_html=True)
        row[4].markdown(f"<div class='row-compact'>{scores.get('critical', '')}</div>", unsafe_allow_html=True)
        row[5].markdown(f"<div class='row-compact'>{scores.get('neutral', '')}</div>", unsafe_allow_html=True)
        row[6].markdown(f"<div class='row-compact'>{scores.get('positive', '')}</div>", unsafe_allow_html=True)
        if row[7].button("보기", key=f"preview_{f.name}") and entry:
            st.session_state["selected_file_name"] = f.name
        report_text = entry.get("report_md", "") if entry else ""
        row[8].download_button(
            label="파일열기",
            data=report_text or "",
            file_name=f"{f.name}.report.md",
            mime="text/markdown",
            key=f"dl_{f.name}",
            use_container_width=True,
        )

    st.session_state["selected_file_ids"] = list(selected_ids)

    st.markdown("<div class='compact'>", unsafe_allow_html=True)
    action_cols = st.columns([5, 1, 1, 1, 1], gap="small")
    action_cols[0].markdown("<div class='muted'>선택 후 평가를 실행하세요.</div>", unsafe_allow_html=True)
    evaluate_selected = action_cols[1].button("선택 평가", use_container_width=True)
    evaluate_all = action_cols[2].button("전체 평가", use_container_width=True)
    load_history = action_cols[3].button("히스토리", use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

    evaluator = Evaluator(api_key=api_key, semaphore=threading.Semaphore(2))
    prompt_step1 = BASE_PROMPT
    prompt_step2 = BASE_PROMPT
    step1_hash = hash_prompt(prompt_step1)
    step2_hash = hash_prompt(prompt_step2)

    if evaluate_selected or evaluate_all:
        target_files = filtered_files if evaluate_all else [f for f in files if f.name in selected_ids]
        if not target_files:
            st.warning("평가할 파일을 선택하세요.")
            return

        results: List[Dict[str, Any]] = []
        failures: List[Dict[str, str]] = []
        progress = st.progress(0)
        progress_text = st.empty()
        completed = 0

        def run_file(file_obj):
            content = file_obj.getvalue().decode("utf-8", errors="replace")
            return evaluate_one(
                evaluator,
                content,
                file_obj.name,
                step1_hash,
                step2_hash,
                force_rerun,
                cache,
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_file = {executor.submit(run_file, f): f for f in target_files}
            for future in concurrent.futures.as_completed(future_to_file):
                file_obj = future_to_file[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    error_info = format_error_info(exc, file_obj.name)
                    results.append(
                        {"status": STATUS_FAILED, "error": error_info, "file_name": file_obj.name}
                    )
                    failures.append(error_info)
                completed += 1
                progress.progress(completed / len(target_files))
                progress_text.write(f"진행: {completed}/{len(target_files)}")

        for res in results:
            file_name = res.get("file_name", "")
            if res.get("status") == STATUS_DONE:
                st.session_state["status_map"][file_name] = STATUS_DONE
            elif res.get("status") == STATUS_SKIPPED:
                st.session_state["status_map"][file_name] = STATUS_SKIPPED
            else:
                if file_name:
                    st.session_state["status_map"][file_name] = STATUS_FAILED

        if failures:
            st.error(
                "\n".join(
                    f"{f['file_name']} | {f['type']} | {f['message']}" for f in failures
                )
            )
        st.rerun()

    if load_history:
        st.info("세션 캐시 기준으로 히스토리를 표시합니다.")

    selected_name = st.session_state.get("selected_file_name")
    entry = cache_by_name.get(selected_name) if selected_name else None
    render_preview_panel(entry)


if __name__ == "__main__":
    main()
