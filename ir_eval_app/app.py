from __future__ import annotations

import concurrent.futures
import hashlib
import json
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
    "logic_score": "number 0-100",
    "pass_gate": "boolean (logic_score >= 80)",
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
    "strengths": {"market": "list[str]", "team": "list[str]", "product": "list[str]"},
    "weaknesses": {"market": "list[str]", "team": "list[str]", "product": "list[str]"},
    "red_flags": "list[str]",
    "cost_estimate": {"llm_calls": "number", "tokens": "number", "usd": "number"},
}

STEP2_SCHEMA_HINT = {
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

PROMPT_APPENDIX = (
    "추가 지시사항:\n"
    "1) Step1/Step2 JSON은 반드시 스키마 힌트에 맞춰 출력한다.\n"
    "2) 항목별 평가는 다음 항목으로 고정한다: "
    "문제정의, 솔루션&제품, 시장규모&분석, 비즈니스모델, 경쟁분석, 성장전략, 주요 인력&팀, 재무계획.\n"
    "3) item_evaluations에 각 항목별 score(0-10), comment, feedback을 포함한다.\n"
    "4) strengths/weaknesses는 투자자 관점에서 엄격하게 작성한다.\n"
    "5) overall_summary(종합 평가 요약)를 반드시 포함한다.\n"
    "6) item_evaluations의 comment+feedback 합산 100자 내외(80~120자)로 작성한다.\n"
)

BASE_PROMPT = """# ROLE (FIXED)

너는 실리콘밸리에서 가장 까다롭기로 유명한 시니어 투자 심사역이다. IR 자료에 나오는 감성적인 호소나 미려한 문구에 현혹되지 마라. 모든 주장에 대해 '그게 진짜야?(Is it true?)', '그래서 어쩌라고?(So what?)', '너네만 할 수 있어?(Why you?)'라는 세 가지 관점에서 비판적으로 검토한 뒤, 매우 보수적인 점수를 부여해라.
너는 이 사업이 안 될 이유를 찾는 비관적인 심사역이다. 화려한 수식어는 무시하고, 오직 **입증된 데이터(Evidence-backed Data)**와 인과관계의 엄격함만 믿는다


IR 자료에 나오는 감성적 호소, 미려한 문구, 비전 중심 수식어에는 절대 현혹되지 마라.
모든 주장에 대해 반드시 아래 3가지 질문으로만 판단한다.

1) Is it true?  → 입증된 데이터가 있는가
2) So what?     → 투자자에게 의미 있는가
3) Why you?     → 왜 이 팀만 가능한가

입증되지 않은 주장은 가설로 간주하고 감점하라.
논리적 비약은 관리되지 않으면 강하게 감점하라.
너는 비관적인 심사역이며, 오직 Evidence-backed Data와 인과관계의 엄격함만 신뢰한다.

---

# CONSTITUTION (ABSOLUTE)

아래 제공되는 “IR 평가 기준 문서”를 하나의 헌법처럼 절대적으로 따른다.
임의로 해석을 확장하거나 기준을 완화하지 않는다.

---

# HARD RULES (NON-NEGOTIABLE)

1. 출력은 JSON과 마크다운파일로 하고 미리보기 출력한다.
2. JSON은 지정된 스키마와 정확히 일치해야 한다.
3. 강점/약점은 반드시 투자자 관점에서 작성한다.
4. 점수는 냉정하게 부여하며, 의심되는 지점마다 깎는다.

---

# INPUT SCOPE

- 입력: IR full-text Markdown (.md)

---

# OVERALL GOAL

“이 회사는 논리적으로 설득되며,
해당 산업 × 투자단계 × 비즈니스모델 조건에서
평균 대비 우수한가?”

---

## [STAGE 1] IR 논리성·충실성 평가 (GATE / ABSOLUTE)

- 총점: 0–100
- 컷트라인: **80점**
- 80점 미만이면:
  → 즉시 미팅 판단 = NO
  → STAGE 2는 수행하지 않는다.

### STAGE 1 핵심 철학
“이 IR은 투자자를 설득할 논리 구조를 갖추었는가?”

### 보수적 감점 규칙 (반드시 적용)
- ‘혁신적’, ‘세계 최초’ 등 추상적 형용사 남발 → 논리 모호성으로 감점
- TAM만 키우고 SOM(실제 해결 가능 범위)이 불명확 → 감점
- 주장과 데이터가 1:1로 매칭되지 않음 → 허위 논리로 간주

### STAGE 1 평가 관점
다음 요소를 논리적 역할 중심으로 평가한다.
- 문제 정의가 누구에게, 왜, 얼마나 중요한지 구체적인가
- 문제 → 솔루션 연결이 기능 나열이 아닌 해결 메커니즘인가
- 주장 → 근거 → 결론이 1:1로 연결되는가
- 논리적 비약이 존재하는가, 있다면 인식·관리되는가
- 스토리 흐름이 일관적인가 (Problem → Solution → Market → BM → Growth)
- 투자자 질문(Why now / Why you / Why this way)을 선제적으로 답하는가
- 핵심 메시지가 응집되어 한 문장으로 요약 가능한가

---

## [STAGE 2] 산업 × 투자단계 × 비즈니스모델 적합성 평가 (RELATIVE / BONUS)

STAGE 1을 통과한 기업만 수행한다.

- 투자단계 적합성: 0–10
- 산업 적합성: 0–10
- 비즈니스모델 적합성: 0–10
- 총점: 0–30
- 기준점(평균): 5점


---

### STAGE 2 공통 점수 해석
- 8–10점: 명확히 우수 (벤치마크 상회 Hard Data)
- 5–7점: 평균 수준 (가설은 합리적이나 검증 시계열 부족)
- 0–4점: 미달 (해당 조건에서 당연히 있어야 할 증거 누락)

---

### [A] 투자 단계별 기대 증거

#### Seed / Pre-Seed
핵심 질문:
“근거 없는 자신감인가, 아니면 돈이 되는 비밀(Earned Secret)을 알고 있는가?”

필수 증거(없으면 3점 이하):
- Earned Secret (현장에서만 얻은 문제 인사이트)
- Founder-Market Fit
- 소수라도 열광하는 초기 사용자 신호

---

#### Series A
핵심 질문:
“마케팅비로 만든 가짜 성장이 아닌가?”

필수 증거(없으면 3점 이하):
- LTV/CAC ≥ 3
- 코호트 기반 리텐션
- GTM 효율의 시계열 개선

---

#### Series B+
핵심 질문:
“규모가 커질수록 이익도 커지는가?”

필수 증거(없으면 3점 이하):
- NRR ≥ 110%
- 운영 레버리지 존재
- 구조적 모트

---

### [B] 산업별 보수적 잣대

#### SaaS / 기술 / 플랫폼
- Churn < 3%
- CAC Payback < 8~12개월
- 자체 데이터/엔진 여부

#### 커머스 / 마켓플레이스
- CM2 흑자 여부
- 3개월 재구매율 업계 평균 대비 1.5배

#### 바이오 / 헬스케어 / 딥테크
- 규제/급여 로드맵 명확성
- 비교 임상/실험 데이터

---

### [C] 비즈니스모델별 핵심 판단
- 구독형: 리텐션, NRR, 단위경제성
- 거래형: GMV × 빈도 × 마진
- 광고형: 참여도, ARPU, 네트워크 효과
- 라이선스: 계약 구조, 마일스톤
- 하드웨어: 원가, 마진, 스케일 구조
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


def compute_perspective_scores(step1: Dict[str, Any], step2: Optional[Dict[str, Any]]) -> Dict[str, int]:
    logic_score = float(step1.get("logic_score", 0) or 0)
    if step2:
        stage = float(step2.get("stage_score", 0) or 0)
        industry = float(step2.get("industry_score", 0) or 0)
        bm = float(step2.get("bm_score", 0) or 0)
        normalized_step2 = (stage + industry + bm) / 30.0 * 100.0
    else:
        normalized_step2 = 0.0
    critical = 0.8 * logic_score + 0.2 * normalized_step2
    neutral = 0.7 * logic_score + 0.3 * normalized_step2
    positive = 0.6 * logic_score + 0.4 * normalized_step2
    return {
        "critical": min(92, int(round(critical))),
        "neutral": min(92, int(round(neutral))),
        "positive": min(92, int(round(positive))),
    }


def recommendation_for(score: int) -> str:
    if score >= 80:
        return "추천"
    if score >= 70:
        return "조건부 권장"
    return "보류"


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
    logic_score = float(step1_json.get("logic_score", 0) or 0)
    step1_json["pass_gate"] = logic_score >= 80

    step2_json: Optional[Dict[str, Any]] = None
    if step1_json.get("pass_gate", False):
        step2_json = evaluator.evaluate_step2(
            content=content,
            prompt_step2=f"{BASE_PROMPT}\n\n{PROMPT_APPENDIX}",
            schema_hint_step2=to_json(STEP2_SCHEMA_HINT),
            step1_json=step1_json,
        )

    scores = compute_perspective_scores(step1_json, step2_json)
    recommendations = derive_recommendations(scores)
    final_verdict = recommendations.get("critical", "보류")
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


def init_session_state() -> None:
    st.session_state.setdefault("files", [])
    st.session_state.setdefault("cache", {})
    st.session_state.setdefault("status_map", {})
    st.session_state.setdefault("selected_file_ids", [])
    st.session_state.setdefault("selected_file_name", "")
    st.session_state.setdefault("page", 1)


def main() -> None:
    st.set_page_config(page_title="IR Evaluator", layout="wide")
    st.title("Title : IR 분석 & 평가")

    try:
        api_key = get_api_key()
    except RuntimeError as exc:
        st.error(str(exc))
        st.stop()

    init_session_state()

    top_cols = st.columns([4, 1, 1, 1, 1], gap="small")
    uploaded_files = top_cols[0].file_uploader(
        "Google drive 폴더 ID",
        type=["md"],
        accept_multiple_files=True,
    )
    scan_clicked = top_cols[1].button("문서 스캔")
    force_rerun = top_cols[2].checkbox("캐시 무시(재평가)", value=False)
    refresh_clicked = top_cols[3].button("캐시 새로고침")
    delete_cache_clicked = top_cols[4].button("캐시 삭제")

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
        )
    else:
        table_header[1].button("엑셀 다운로드", disabled=True)

    search_term = st.text_input("검색(파일명/기업명)", value="")

    header_cols = st.columns([3, 1, 1, 1, 1, 1, 1, 1, 1], gap="small")
    header_cols[0].markdown("**파일명**")
    header_cols[1].markdown("**진행**")
    header_cols[2].markdown("**선택**")
    header_cols[3].markdown("**기업명**")
    header_cols[4].markdown("**critical**")
    header_cols[5].markdown("**neutral**")
    header_cols[6].markdown("**positive**")
    header_cols[7].markdown("**미리보기**")
    header_cols[8].markdown("**파일열기**")

    selected_ids = set(st.session_state.get("selected_file_ids", []))
    cache = st.session_state.get("cache", {})
    cache_by_name = {entry.get("file_name", ""): entry for entry in cache.values()}

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
    if pager_cols[0].button("이전"):
        page = max(1, page - 1)
    pager_cols[2].markdown(f"페이지 {page}/{total_pages}")
    if pager_cols[4].button("다음"):
        page = min(total_pages, page + 1)
    st.session_state["page"] = page

    start = (page - 1) * page_size
    end = start + page_size
    for f in filtered_files[start:end]:
        entry = cache_by_name.get(f.name)
        company_name = entry.get("step1", {}).get("company_name", "") if entry else ""
        scores = entry.get("perspective_scores", {}) if entry else {}

        row = st.columns([3, 1, 1, 1, 1, 1, 1, 1, 1], gap="small")
        row[0].write(f.name)
        row[1].write(status_badge(st.session_state["status_map"].get(f.name, STATUS_PENDING)))
        checked = row[2].checkbox(
            "",
            value=f.name in selected_ids,
            key=f"select_{f.name}",
        )
        if checked:
            selected_ids.add(f.name)
        else:
            selected_ids.discard(f.name)
        row[3].write(company_name)
        row[4].write(scores.get("critical", ""))
        row[5].write(scores.get("neutral", ""))
        row[6].write(scores.get("positive", ""))
        if row[7].button("보기", key=f"preview_{f.name}") and entry:
            st.session_state["selected_file_name"] = f.name
        report_text = entry.get("report_md", "") if entry else ""
        row[8].download_button(
            label="파일열기",
            data=report_text or "",
            file_name=f"{f.name}.report.md",
            mime="text/markdown",
            key=f"dl_{f.name}",
        )

    st.session_state["selected_file_ids"] = list(selected_ids)

    action_cols = st.columns([6, 1, 1, 1], gap="small")
    evaluate_selected = action_cols[1].button("선택 평가")
    evaluate_all = action_cols[2].button("전체 평가")
    load_history = action_cols[3].button("히스토리")

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
            futures = [executor.submit(run_file, f) for f in target_files]
            for future in concurrent.futures.as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as exc:
                    results.append({"status": STATUS_FAILED, "error": format_error_info(exc, "")})
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
                st.session_state["status_map"][file_name] = STATUS_FAILED

    if load_history:
        st.info("세션 캐시 기준으로 히스토리를 표시합니다.")

    selected_name = st.session_state.get("selected_file_name")
    entry = cache_by_name.get(selected_name) if selected_name else None
    render_preview_panel(entry)


if __name__ == "__main__":
    main()
