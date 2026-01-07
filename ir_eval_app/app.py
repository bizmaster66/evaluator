from __future__ import annotations

import concurrent.futures
import json
import threading
import time
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional

import openpyxl
import streamlit as st
from dateutil import tz
from google.oauth2 import service_account

from src.cache_store import CacheStore
from src.config import (
    MODEL_NAME,
    PROMPT_STEP1_PATH,
    PROMPT_STEP2_PATH,
    JSON_RESULTS_FOLDER_NAME,
    RESULTS_FOLDER_NAME,
    hash_prompt,
    load_prompt,
    md5_text,
    to_json,
)
from src.drive_client import DriveClient
from src.evaluator import Evaluator
from src.report_writer import render_report
from src.utils import hash_cache_key

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

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
    "file_id",
    "file_name",
    "source_folder",
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
    "report_file_url",
    "result_json_url",
]

STATUS_PENDING = "대기"
STATUS_RUNNING = "진행"
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
    "추가 지시사항:\\n"
    "1) Step1/Step2 JSON은 반드시 스키마 힌트에 맞춰 출력한다.\\n"
    "2) 항목별 평가는 다음 항목으로 고정한다: "
    "문제정의, 솔루션&제품, 시장규모&분석, 비즈니스모델, 경쟁분석, 성장전략, 주요 인력&팀, 재무계획.\\n"
    "3) item_evaluations에 각 항목별 score(0-10), comment, feedback을 포함한다.\\n"
    "4) strengths/weaknesses는 투자자 관점에서 엄격하게 작성한다.\\n"
    "5) overall_summary(종합 평가 요약)를 반드시 포함한다.\\n"
)


def normalize_folder_id(value: str) -> str:
    text = (value or "").strip()
    if "/folders/" in text:
        return text.split("/folders/", 1)[1].split("?", 1)[0].split("/", 1)[0]
    if "id=" in text:
        return text.split("id=", 1)[1].split("&", 1)[0]
    return text


def status_badge(status: str) -> str:
    mapping = {
        STATUS_DONE: "✅완료",
        STATUS_PENDING: "🕒대기",
        STATUS_FAILED: "⚠️실패",
        STATUS_RUNNING: "🔄진행중",
        STATUS_SKIPPED: "✅완료",
    }
    return mapping.get(status, status or "-")


def short_text(text: str, limit: int = 120) -> str:
    value = (text or "").strip()
    if len(value) <= limit:
        return value
    return value[:limit].rstrip() + "..."


def load_credentials() -> service_account.Credentials:
    import json
    import streamlit as st
    from google.oauth2 import service_account

    # 1) Preferred sectioned secrets
    info = None
    if "google" in st.secrets and "service_account_json" in st.secrets["google"]:
        info = st.secrets["google"]["service_account_json"]
    # 2) Legacy top-level
    elif "service_account_json" in st.secrets:
        info = st.secrets["service_account_json"]
    # 3) Legacy dict fields
    elif "gcp_service_account" in st.secrets:
        info = dict(st.secrets["gcp_service_account"])

    if info is None:
        raise RuntimeError("Missing service_account_json in Streamlit secrets")

    # dict -> use directly
    if isinstance(info, dict):
        sa_info = info
    elif isinstance(info, str):
        s = info.strip()
        # remove one extra wrapping quote layer if present
        if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
            s = s[1:-1].strip()
        try:
            sa_info = json.loads(s)
        except Exception as e:
            # safe diagnostics (no secret leak)
            starts = s.lstrip().startswith("{")
            ends = s.rstrip().endswith("}")
            length = len(s)
            raise RuntimeError(
                f"Invalid service_account_json JSON in Streamlit secrets "
                f"(starts_with_{{={starts}}}, ends_with_}}={ends}, length={length})"
            ) from e
    else:
        raise RuntimeError(f"Unsupported service_account_json type: {type(info)}")

    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=[
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        ],
    )
    return creds


def get_api_key() -> str:
    api_key = None
    if st.secrets.get("gemini") and st.secrets["gemini"].get("api_key"):
        api_key = st.secrets["gemini"]["api_key"]
    elif st.secrets.get("gemini_api_key"):
        api_key = st.secrets["gemini_api_key"]
    elif st.secrets.get("gemini") and st.secrets["gemini"].get("GEMINI_API_KEY"):
        api_key = st.secrets["gemini"]["GEMINI_API_KEY"]

    if not api_key:
        raise RuntimeError("Missing gemini api key in Streamlit secrets")
    return api_key


def kst_now() -> str:
    kst = tz.gettz("Asia/Seoul")
    return datetime.now(tz=kst).strftime("%Y-%m-%d %H:%M:%S")


def compute_cache_key(
    file_id: str,
    content: str,
    modified_time: str,
    step1_hash: str,
    step2_hash: str,
) -> str:
    parts = [file_id, md5_text(content), modified_time, step1_hash, step2_hash, MODEL_NAME]
    return hash_cache_key(parts)


def ensure_results_folder(drive: DriveClient, source_folder_id: str) -> str:
    drive_id = drive.get_drive_id(source_folder_id)
    return drive.get_or_create_folder(RESULTS_FOLDER_NAME, parent_id=source_folder_id, drive_id=drive_id)


def ensure_json_folder(drive: DriveClient, results_folder_id: str) -> str:
    drive_id = drive.get_drive_id(results_folder_id)
    return drive.get_or_create_folder(JSON_RESULTS_FOLDER_NAME, parent_id=results_folder_id, drive_id=drive_id)


def compute_final_scores(step1: Dict[str, Any], step2: Optional[Dict[str, Any]]) -> Dict[str, float]:
    logic_score = float(step1.get("logic_score", 0) or 0)
    if step2:
        stage = float(step2.get("stage_score", 0) or 0)
        industry = float(step2.get("industry_score", 0) or 0)
        bm = float(step2.get("bm_score", 0) or 0)
        normalized_step2 = (stage + industry + bm) / 30.0 * 100.0
    else:
        normalized_step2 = 0.0
    final_score = 0.7 * logic_score + 0.3 * normalized_step2
    final_score = max(0.0, min(92.0, final_score))
    return {
        "conservative": round(final_score, 2),
        "neutral": round(final_score, 2),
        "optimistic": round(final_score, 2),
    }


def compute_perspective_scores(step1: Dict[str, Any], step2: Optional[Dict[str, Any]]) -> Dict[str, int]:
    logic_score = float(step1.get("logic_score", 0) or 0)
    if step2:
        stage = float(step2.get("stage_score", 0) or 0)
        industry = float(step2.get("industry_score", 0) or 0)
        bm = float(step2.get("bm_score", 0) or 0)
        normalized_step2 = (stage + industry + bm) / 30.0 * 100.0
    else:
        normalized_step2 = 0.0
    critical = 0.7 * logic_score + 0.3 * normalized_step2
    neutral = 0.6 * logic_score + 0.4 * normalized_step2
    positive = 0.5 * logic_score + 0.5 * normalized_step2
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


def evaluate_file(
    drive: DriveClient,
    evaluator: Evaluator,
    cache: CacheStore,
    folder_id: str,
    file_meta: Dict[str, Any],
    prompt_step1: str,
    prompt_step2: str,
    step1_hash: str,
    step2_hash: str,
    force_rerun: bool,
) -> Dict[str, Any]:
    file_id = file_meta["id"]
    file_name = file_meta["name"]
    cache_key = ""
    try:
        modified_time = file_meta.get("modifiedTime", "")

        content = _retry(stage="download", func=lambda: drive.get_file_text(file_id))
        cache_key = compute_cache_key(file_id, content, modified_time, step1_hash, step2_hash)
        cached = cache.get(cache_key)
        if cached and not force_rerun:
            return {"status": STATUS_SKIPPED, "file": file_meta, "cache": cached}

        step1_json = _retry(
            stage="step1",
            func=lambda: evaluator.evaluate_step1(
                content=content,
                prompt_step1=f"{prompt_step1}\n\n{PROMPT_APPENDIX}",
                schema_hint_step1=to_json(STEP1_SCHEMA_HINT),
            ),
        )
        logic_score = float(step1_json.get("logic_score", 0) or 0)
        step1_json["pass_gate"] = logic_score >= 80

        step2_json: Optional[Dict[str, Any]] = None
        if step1_json.get("pass_gate", False):
            step2_json = _retry(
                stage="step2",
                func=lambda: evaluator.evaluate_step2(
                    content=content,
                    prompt_step2=f"{prompt_step2}\n\n{PROMPT_APPENDIX}",
                    schema_hint_step2=to_json(STEP2_SCHEMA_HINT),
                    step1_json=step1_json,
                ),
            )

        final_scores = compute_final_scores(step1_json, step2_json)
        perspective_scores = compute_perspective_scores(step1_json, step2_json)
        recommendations = derive_recommendations(perspective_scores)
        final_verdict = recommendations.get("critical", "보류")
        report_md = render_report(
            file_name,
            step1_json,
            step2_json,
            perspective_scores,
            recommendations,
            final_verdict,
        )
        report_name = f"{file_name}.report.md"
        report_id = _retry(stage="upload_report", func=lambda: drive.upload_markdown(folder_id, report_name, report_md))
        report_url = _retry(stage="upload_report", func=lambda: drive.get_file_link(report_id))

        json_folder_id = ensure_json_folder(drive, folder_id)
        step1_json_name = f"{file_name}.step1.json"
        step1_json_id = _retry(
            stage="upload_report",
            func=lambda: drive.upload_text(
                json_folder_id, step1_json_name, json.dumps(step1_json, ensure_ascii=True, indent=2), "application/json"
            ),
        )
        step1_json_url = _retry(stage="upload_report", func=lambda: drive.get_file_link(step1_json_id))

        step2_json_id = ""
        step2_json_url = ""
        if step2_json:
            step2_json_name = f"{file_name}.step2.json"
            step2_json_id = _retry(
                stage="upload_report",
                func=lambda: drive.upload_text(
                    json_folder_id,
                    step2_json_name,
                    json.dumps(step2_json, ensure_ascii=True, indent=2),
                    "application/json",
                ),
            )
            step2_json_url = _retry(stage="upload_report", func=lambda: drive.get_file_link(step2_json_id))
        result_payload = {
            "file_id": file_id,
            "file_name": file_name,
            "timestamp": kst_now(),
            "company_name": step1_json.get("company_name", ""),
            "company_description": step1_json.get("one_line_summary", ""),
            "scores": perspective_scores,
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
        result_json_name = f"{file_name}.result.json"
        result_json_id = _retry(
            stage="upload_report",
            func=lambda: drive.upload_text(
                json_folder_id,
                result_json_name,
                json.dumps(result_payload, ensure_ascii=True, indent=2),
                "application/json",
            ),
        )
        result_json_url = _retry(stage="upload_report", func=lambda: drive.get_file_link(result_json_id))

        cache_entry = {
            "file_id": file_id,
            "file_name": file_name,
            "source_folder": folder_id,
            "report_file_id": report_id,
            "report_file_url": report_url,
            "step1_json_file_id": step1_json_id,
            "step1_json_file_url": step1_json_url,
            "step2_json_file_id": step2_json_id,
            "step2_json_file_url": step2_json_url,
            "result_json_file_id": result_json_id,
            "result_json_file_url": result_json_url,
            "timestamp": kst_now(),
            "summary": step1_json.get("one_line_summary", ""),
            "step1": step1_json,
            "step2": step2_json,
            "final_scores": final_scores,
            "perspective_scores": perspective_scores,
            "recommendations": recommendations,
            "final_verdict": final_verdict,
        }
        _retry(stage="save_cache", func=lambda: cache.set(cache_key, cache_entry))

        return {
            "status": STATUS_DONE,
            "file": file_meta,
            "cache": cache_entry,
            "report_md": report_md,
        }
    except Exception as exc:
        err_info = format_error_info(exc, file_id, file_name)
        fail_entry = {
            "file_id": file_id,
            "file_name": file_name,
            "source_folder": folder_id,
            "timestamp": kst_now(),
            "status": STATUS_FAILED,
            "error": err_info,
        }
        if cache_key:
            cache.set(cache_key, fail_entry)
        return {"status": STATUS_FAILED, "file": file_meta, "error": err_info}


def _retry(stage: str, func, retries: int = 2) -> Any:
    last_exc: Optional[Exception] = None
    for _ in range(retries + 1):
        try:
            return func()
        except Exception as exc:
            last_exc = exc
            time.sleep(0.6)
    if last_exc:
        raise wrap_stage_error(stage, last_exc) from last_exc
    raise RuntimeError("Unknown error")


def wrap_stage_error(stage: str, exc: Exception) -> Exception:
    return RuntimeError(f"stage={stage} | {exc}")


def format_error_info(exc: Exception, file_id: str, file_name: str) -> Dict[str, str]:
    message = str(exc).replace("\n", " ")[:300]
    return {
        "type": exc.__class__.__name__,
        "message": message,
        "file_id": file_id,
        "file_name": file_name,
    }


def build_sheet_row(cache_entry: Dict[str, Any], source_folder_id: str) -> Dict[str, Any]:
    step1 = cache_entry.get("step1", {})
    perspective_scores = cache_entry.get("perspective_scores", {})
    recommendations = cache_entry.get("recommendations", {})
    step2 = cache_entry.get("step2", {})
    return {
        "timestamp(KST)": cache_entry.get("timestamp", kst_now()),
        "file_id": cache_entry.get("file_id", ""),
        "file_name": cache_entry.get("file_name", ""),
        "source_folder": source_folder_id,
        "company_name": step1.get("company_name", ""),
        "company_description": step1.get("one_line_summary", ""),
        "score_critical": perspective_scores.get("critical", ""),
        "score_neutral": perspective_scores.get("neutral", ""),
        "score_positive": perspective_scores.get("positive", ""),
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
        "final_verdict": cache_entry.get("final_verdict", ""),
        "report_file_url": cache_entry.get("report_file_url", ""),
        "result_json_url": cache_entry.get("result_json_file_url", ""),
    }


def cache_to_excel_bytes(cache: CacheStore, source_folder_id: str) -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "IR_EVAL"
    ws.append(SHEET_COLUMNS)
    for entry in cache.data.get("items", {}).values():
        row = build_sheet_row(entry, source_folder_id)
        ws.append([row.get(col, "") for col in SHEET_COLUMNS])
    buffer = BytesIO()
    wb.save(buffer)
    return buffer.getvalue()


def excel_filename(source_folder_id: str) -> str:
    stamp = datetime.now(tz=tz.UTC).strftime("%Y%m%d_%H%M")
    return f"ir_eval_{source_folder_id}_{stamp}.xlsx"


def get_report_text(drive: DriveClient, entry: Dict[str, Any]) -> str:
    if entry.get("report_md"):
        return entry["report_md"]
    report_id = entry.get("report_file_id")
    if report_id:
        return drive.get_file_text(report_id)
    return ""


def render_results_list(drive: DriveClient, cache: CacheStore, folder_id: str) -> None:
    items = list(cache.data.get("items", {}).values())
    if not items:
        st.info("히스토리가 없습니다.")
        return
    st.subheader("결과 목록")
    items_sorted = sorted(items, key=lambda x: x.get("timestamp", ""), reverse=True)
    result_rows = []
    result_label_map = {}
    for entry in items_sorted:
        name = entry.get("file_name", "")
        entry_id = entry.get("file_id", "")
        scores = entry.get("perspective_scores", {})
        recs = entry.get("recommendations", {})
        result_rows.append(
            {
                "file_name": name,
                "timestamp": entry.get("timestamp", ""),
                "critical": scores.get("critical", ""),
                "neutral": scores.get("neutral", ""),
                "positive": scores.get("positive", ""),
                "recommendation": recs.get("critical", ""),
                "report_url": entry.get("report_file_url", ""),
            }
        )
        result_label_map[f"{name} [{entry_id[:6]}]"] = entry

    st.dataframe(result_rows, use_container_width=True, height=320)
    selected_result = st.selectbox("결과 선택", list(result_label_map.keys()))
    entry = result_label_map.get(selected_result)
    if entry:
        cols = st.columns([2, 2, 2, 6])
        if cols[0].button("결과보기"):
            st.session_state["last_report"] = get_report_text(drive, entry)
        report_text = get_report_text(drive, entry)
        cols[1].download_button(
            label="다운로드",
            data=report_text or "",
            file_name=f"{entry.get('file_name','')}.report.md",
            mime="text/markdown",
        )
        result_json_id = entry.get("result_json_file_id", "")
        if result_json_id:
            result_json_text = drive.get_file_text(result_json_id)
            cols[2].download_button(
                label="JSON",
                data=result_json_text,
                file_name=f"{entry.get('file_name','')}.result.json",
                mime="application/json",
            )
        if entry.get("report_file_url"):
            cols[3].markdown(f"[리포트 열기]({entry['report_file_url']})")

    excel_bytes = cache_to_excel_bytes(cache, folder_id)
    st.download_button(
        label="엑셀 다운로드",
        data=excel_bytes,
        file_name=excel_filename(folder_id),
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def init_session_state() -> None:
    st.session_state.setdefault("folder_id", "")
    st.session_state.setdefault("files", [])
    st.session_state.setdefault("results", [])
    st.session_state.setdefault("selected_file_id", "")
    st.session_state.setdefault("selected_file_name", "")
    st.session_state.setdefault("selected_file_ids", [])
    st.session_state.setdefault("last_report", "")
    st.session_state.setdefault("status_map", {})
    st.session_state.setdefault("rerun_file_id", "")


def render_sidebar(drive: DriveClient) -> Dict[str, Any]:
    st.sidebar.header("#사이드바")
    folder_input = st.sidebar.text_input("Google drive 폴더 ID", value=st.session_state.get("folder_id", ""))
    folder_id = normalize_folder_id(folder_input)
    st.session_state["folder_id"] = folder_id

    scan_clicked = st.sidebar.button("폴더 스캔")
    refresh_clicked = st.sidebar.button("캐시 새로고침")
    delete_cache_clicked = st.sidebar.button("캐시 삭제")
    if refresh_clicked and folder_id:
        st.session_state["cache_reload"] = True
    if delete_cache_clicked and folder_id:
        st.session_state["cache_delete"] = True

    st.sidebar.subheader("파일 목록 리스트")
    files = st.session_state.get("files", [])
    file_rows = []
    file_map = {}
    for f in files:
        status = status_badge(st.session_state["status_map"].get(f["id"], STATUS_PENDING))
        file_rows.append({"파일명": f["name"], "진행": status})
        file_map[f"{f['name']} [{f['id'][:6]}]"] = f["id"]
    st.sidebar.dataframe(file_rows, use_container_width=True, height=240)

    if file_map:
        labels = list(file_map.keys())
        selected_labels = st.sidebar.multiselect(
            "평가 대상 선택",
            labels,
            default=[label for label in labels if file_map[label] in st.session_state.get("selected_file_ids", [])],
        )
        st.session_state["selected_file_ids"] = [file_map[label] for label in selected_labels]

        default_index = 0
        current_id = st.session_state.get("selected_file_id")
        if current_id:
            for idx, label in enumerate(labels):
                if file_map[label] == current_id:
                    default_index = idx
                    break
        selected_label = st.sidebar.selectbox("미리보기 선택", labels, index=default_index)
        st.session_state["selected_file_id"] = file_map.get(selected_label, "")
        st.session_state["selected_file_name"] = selected_label.split(" [", 1)[0] if selected_label else ""

    st.sidebar.subheader("평가 실행")
    force_rerun = st.sidebar.checkbox("캐시 무시(재평가)", value=False)
    btn_cols = st.sidebar.columns(3)
    evaluate_selected = btn_cols[0].button("선택 평가")
    evaluate_all = btn_cols[1].button("전체 평가")
    load_history = btn_cols[2].button("히스토리")

    return {
        "folder_id": folder_id,
        "scan_clicked": scan_clicked,
        "force_rerun": force_rerun,
        "evaluate_selected": evaluate_selected,
        "evaluate_all": evaluate_all,
        "load_history": load_history,
        "delete_cache_clicked": delete_cache_clicked,
    }


def render_main_header(cache: Optional[CacheStore], folder_id: str) -> None:
    cols = st.columns([3, 1])
    cols[0].header("평가 리포트")
    if cache:
        excel_bytes = cache_to_excel_bytes(cache, folder_id)
        cols[1].download_button(
            label="엑셀 다운로드",
            data=excel_bytes,
            file_name=excel_filename(folder_id),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        cols[1].button("엑셀 다운로드", disabled=True)


def render_report_table(drive: DriveClient, cache: Optional[CacheStore]) -> None:
    if not cache:
        st.info("리포트가 없습니다.")
        return
    items = list(cache.data.get("items", {}).values())
    if not items:
        st.info("리포트가 없습니다.")
        return
    rows = []
    entry_map = {}
    for entry in sorted(items, key=lambda x: x.get("timestamp", ""), reverse=True):
        step1 = entry.get("step1", {})
        scores = entry.get("perspective_scores", {})
        file_name = entry.get("file_name", "")
        entry_map[file_name] = entry
        rows.append(
            {
                "파일명": file_name,
                "기업명": step1.get("company_name", ""),
                "critical": scores.get("critical", ""),
                "neutral": scores.get("neutral", ""),
                "positive": scores.get("positive", ""),
                "미리보기": "보기",
                ".md 다운로드": "다운로드",
            }
        )

    st.dataframe(rows, use_container_width=True, height=260)
    selected_name = st.selectbox("리포트 선택", list(entry_map.keys()))
    entry = entry_map.get(selected_name)
    action_cols = st.columns([1, 1, 2, 4])
    if action_cols[0].button("보기"):
        st.session_state["selected_file_id"] = entry.get("file_id", "")
        st.session_state["selected_file_name"] = entry.get("file_name", "")
        st.session_state["last_report"] = get_report_text(drive, entry)
    report_text = get_report_text(drive, entry)
    action_cols[1].download_button(
        label=".md 다운로드",
        data=report_text or "",
        file_name=f"{entry.get('file_name','')}.report.md",
        mime="text/markdown",
    )
    if entry.get("report_file_url"):
        action_cols[2].markdown(f"[리포트 열기]({entry['report_file_url']})")
    if entry.get("result_json_file_id"):
        result_json_text = drive.get_file_text(entry["result_json_file_id"])
        action_cols[3].download_button(
            label="JSON 다운로드",
            data=result_json_text,
            file_name=f"{entry.get('file_name','')}.result.json",
            mime="application/json",
        )


def find_selected_entry(cache: Optional[CacheStore]) -> Optional[Dict[str, Any]]:
    if not cache:
        return None
    selected_id = st.session_state.get("selected_file_id")
    selected_name = st.session_state.get("selected_file_name")
    for entry in cache.data.get("items", {}).values():
        if selected_id and entry.get("file_id") == selected_id:
            return entry
        if selected_name and entry.get("file_name") == selected_name:
            return entry
    return None


def render_preview_panel(entry: Optional[Dict[str, Any]]) -> None:
    st.subheader("미리보기")
    if not entry:
        st.info("선택된 리포트가 없습니다.")
        return

    step1 = entry.get("step1", {})
    scores = entry.get("perspective_scores", {})
    company_name = step1.get("company_name") or "기업명 미상"
    title = f"{company_name} 분석 결과"
    st.markdown(
        f"#리포트 제목  {title}  \n"
        f"Critical : {scores.get('critical','')}   "
        f"Neutral : {scores.get('neutral','')}   "
        f"Positive : {scores.get('positive','')}"
    )
    st.markdown(short_text(step1.get("one_line_summary", "")))

    st.markdown("### Title : 종합 평가")
    st.info(step1.get("overall_summary", "(없음)"))

    item_evaluations = step1.get("item_evaluations", {})
    if not item_evaluations:
        st.info("항목별 평가가 없습니다.")
        return

    st.markdown("### 항목별 평가")
    for i in range(0, len(ITEM_KEYS), 2):
        cols = st.columns(2)
        for j, key in enumerate(ITEM_KEYS[i : i + 2]):
            value = item_evaluations.get(key, {})
            text = f"{value.get('comment','')} {value.get('feedback','')}".strip()
            cols[j].markdown(f"**Title : {key}**")
            cols[j].write(short_text(text, 180))


def main() -> None:
    st.set_page_config(page_title="IR Evaluator", layout="wide")

    try:
        credentials = load_credentials()
        api_key = get_api_key()
    except RuntimeError as exc:
        st.error(str(exc))
        st.stop()

    drive = DriveClient(credentials)

    init_session_state()

    ui = render_sidebar(drive)
    folder_id = ui["folder_id"]
    cache = None
    result_folder_id = ""
    if folder_id:
        result_folder_id = ensure_results_folder(drive, folder_id)
        cache = CacheStore(drive, result_folder_id)
        cache.load()

    if st.session_state.pop("cache_reload", False) and folder_id:
        cache = CacheStore(drive, result_folder_id)
        cache.load()

    if st.session_state.pop("cache_delete", False) and folder_id:
        existing = drive.find_file_in_folder(result_folder_id, "cache_index.json", mime_type="application/json")
        if existing:
            drive.service.files().delete(fileId=existing["id"], supportsAllDrives=True).execute()
        cache = CacheStore(drive, result_folder_id)
        cache.load()

    if ui["scan_clicked"] and folder_id:
        with st.spinner("스캔 중..."):
            st.session_state["files"] = drive.list_md_files(folder_id)
            st.session_state["status_map"] = {f["id"]: STATUS_PENDING for f in st.session_state["files"]}

    files = st.session_state.get("files", [])
    if not files:
        st.info("폴더를 스캔하면 .md 파일 목록이 나타납니다.")
        return

    render_main_header(cache, folder_id)
    render_report_table(drive, cache)

    rerun_file_id = st.session_state.get("rerun_file_id")
    evaluate_selected = ui["evaluate_selected"]
    evaluate_all = ui["evaluate_all"]
    load_history = ui["load_history"]
    force_rerun = ui["force_rerun"]
    if rerun_file_id:
        evaluate_selected = True
        force_rerun = True
        st.session_state["rerun_file_id"] = ""

    if evaluate_selected or evaluate_all:
        if evaluate_all:
            target_files = files
        else:
            selected_ids = set(st.session_state.get("selected_file_ids", []))
            target_files = [f for f in files if f["id"] in selected_ids]
        if rerun_file_id:
            target_files = [f for f in files if f["id"] == rerun_file_id]
        if not target_files:
            st.warning("평가할 파일을 선택하세요.")
            return

        prompt_step1 = load_prompt(PROMPT_STEP1_PATH)
        prompt_step2 = load_prompt(PROMPT_STEP2_PATH)
        step1_hash = hash_prompt(prompt_step1)
        step2_hash = hash_prompt(prompt_step2)

        semaphore = threading.Semaphore(2)
        evaluator = Evaluator(api_key=api_key, semaphore=semaphore)

        results: List[Dict[str, Any]] = []
        progress = st.progress(0)
        progress_text = st.empty()
        completed = 0
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                futures = []
                for f in target_files:
                    st.session_state["status_map"][f["id"]] = STATUS_RUNNING
                    futures.append(
                        executor.submit(
                            evaluate_file,
                            drive,
                            evaluator,
                            cache,
                            result_folder_id,
                            f,
                            prompt_step1,
                            prompt_step2,
                            step1_hash,
                            step2_hash,
                            force_rerun,
                        )
                    )
                for future in concurrent.futures.as_completed(futures):
                    try:
                        results.append(future.result())
                    except Exception as exc:
                        results.append(
                            {
                                "status": STATUS_FAILED,
                                "file": {"id": "", "name": ""},
                                "error": format_error_info(exc, "", ""),
                            }
                        )
                    completed += 1
                    progress.progress(completed / len(target_files))
                    progress_text.write(f"진행: {completed}/{len(target_files)}")
        finally:
            if cache:
                cache.save()

        failed = []
        for res in results:
            status = res.get("status")
            file_meta = res.get("file", {})
            file_id = file_meta.get("id", "")
            file_name = file_meta.get("name", "")
            if file_id:
                st.session_state["status_map"][file_id] = status
            if status == STATUS_FAILED and res.get("error"):
                failed.append(res["error"])
            cache_entry = res.get("cache", {})
            if res.get("report_md"):
                st.session_state["last_report"] = res["report_md"]
                st.session_state["selected_file_id"] = cache_entry.get("file_id", file_id)
                st.session_state["selected_file_name"] = cache_entry.get("file_name", file_name)

        if failed:
            st.subheader("실패 상세")
            for item in failed:
                st.write(
                    f"{item.get('type')} | {item.get('message')} | "
                    f"file_id={item.get('file_id')} | file_name={item.get('file_name')}"
                )

        if cache:
            render_report_table(drive, cache)

    if load_history and folder_id:
        if cache:
            render_report_table(drive, cache)

    selected_entry = find_selected_entry(cache)
    render_preview_panel(selected_entry)


if __name__ == "__main__":
    main()
