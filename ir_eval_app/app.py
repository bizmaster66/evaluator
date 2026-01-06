from __future__ import annotations

import concurrent.futures
import json
import threading
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
    "one_line_summary": "string",
    "logic_score": "number 0-100",
    "pass_gate": "boolean (logic_score >= 80)",
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
    "one_line_summary",
    "final_score_conservative",
    "final_score_neutral",
    "final_score_optimistic",
    "meeting_decision",
    "item_scores_json",
    "strengths_json",
    "weaknesses_json",
    "cost_estimate_json",
    "report_file_url",
]

STATUS_PENDING = "대기"
STATUS_RUNNING = "진행"
STATUS_SKIPPED = "스킵"
STATUS_DONE = "완료"
STATUS_FAILED = "실패"


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
    root_id = drive.get_or_create_folder(RESULTS_FOLDER_NAME)
    return drive.get_or_create_folder(source_folder_id, parent_id=root_id)


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
    final_score = max(0.0, min(100.0, final_score))
    return {
        "conservative": round(final_score, 2),
        "neutral": round(final_score, 2),
        "optimistic": round(final_score, 2),
    }


def derive_meeting_decision(step1: Dict[str, Any], final_scores: Dict[str, float]) -> str:
    if not step1.get("pass_gate", False):
        return "NO"
    conservative = float(final_scores.get("conservative", 0))
    if conservative >= 85:
        return "권장"
    if conservative >= 75:
        return "조건부 권장"
    if conservative >= 65:
        return "보류"
    return "NO"


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
    modified_time = file_meta.get("modifiedTime", "")

    content = drive.get_file_text(file_id)
    cache_key = compute_cache_key(file_id, content, modified_time, step1_hash, step2_hash)
    cached = cache.get(cache_key)
    if cached and not force_rerun:
        return {"status": STATUS_SKIPPED, "file": file_meta, "cache": cached}

    step1_json = evaluator.evaluate_step1(
        content=content,
        prompt_step1=prompt_step1,
        schema_hint_step1=to_json(STEP1_SCHEMA_HINT),
    )

    step2_json: Optional[Dict[str, Any]] = None
    if step1_json.get("pass_gate", False):
        step2_json = evaluator.evaluate_step2(
            content=content,
            prompt_step2=prompt_step2,
            schema_hint_step2=to_json(STEP2_SCHEMA_HINT),
            step1_json=step1_json,
        )

    final_scores = compute_final_scores(step1_json, step2_json)
    meeting_decision = derive_meeting_decision(step1_json, final_scores)
    report_md = render_report(file_name, step1_json, step2_json, final_scores, meeting_decision)
    report_name = f"{file_name}.report.md"
    report_id = drive.upload_markdown(folder_id, report_name, report_md)
    report_url = drive.get_file_link(report_id)

    cache_entry = {
        "file_id": file_id,
        "file_name": file_name,
        "source_folder": folder_id,
        "report_file_id": report_id,
        "report_file_url": report_url,
        "timestamp": kst_now(),
        "summary": step1_json.get("one_line_summary", ""),
        "step1": step1_json,
        "step2": step2_json,
        "final_scores": final_scores,
        "meeting_decision": meeting_decision,
    }
    cache.set(cache_key, cache_entry)

    return {
        "status": STATUS_DONE,
        "file": file_meta,
        "cache": cache_entry,
        "report_md": report_md,
    }


def build_sheet_row(cache_entry: Dict[str, Any], source_folder_id: str) -> Dict[str, Any]:
    step1 = cache_entry.get("step1", {})
    final_scores = cache_entry.get("final_scores", {})
    return {
        "timestamp(KST)": cache_entry.get("timestamp", kst_now()),
        "file_id": cache_entry.get("file_id", ""),
        "file_name": cache_entry.get("file_name", ""),
        "source_folder": source_folder_id,
        "one_line_summary": step1.get("one_line_summary", ""),
        "final_score_conservative": final_scores.get("conservative", ""),
        "final_score_neutral": final_scores.get("neutral", ""),
        "final_score_optimistic": final_scores.get("optimistic", ""),
        "meeting_decision": cache_entry.get("meeting_decision", ""),
        "item_scores_json": json.dumps(step1.get("item_scores", {}), ensure_ascii=True),
        "strengths_json": json.dumps(step1.get("strengths", {}), ensure_ascii=True),
        "weaknesses_json": json.dumps(step1.get("weaknesses", {}), ensure_ascii=True),
        "cost_estimate_json": json.dumps(step1.get("cost_estimate", {}), ensure_ascii=True),
        "report_file_url": cache_entry.get("report_file_url", ""),
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
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M")
    return f"ir_eval_{source_folder_id}_{stamp}.xlsx"


def init_session_state() -> None:
    st.session_state.setdefault("files", [])
    st.session_state.setdefault("last_report", "")
    st.session_state.setdefault("status_map", {})
    st.session_state.setdefault("rerun_file_id", "")


def main() -> None:
    st.set_page_config(page_title="IR Evaluator", layout="wide")
    st.title("IR 평가 앱")

    try:
        credentials = load_credentials()
        api_key = get_api_key()
    except RuntimeError as exc:
        st.error(str(exc))
        st.stop()

    drive = DriveClient(credentials)

    init_session_state()

    folder_id = st.text_input("Google Drive 폴더 ID")

    if st.button("폴더 스캔") and folder_id:
        with st.spinner("스캔 중..."):
            st.session_state["files"] = drive.list_md_files(folder_id)
            st.session_state["status_map"] = {f["id"]: STATUS_PENDING for f in st.session_state["files"]}

    files = st.session_state.get("files", [])
    st.subheader("파일 목록")
    if not files:
        st.info("폴더를 스캔하면 .md 파일 목록이 나타납니다.")
        return

    selections = {}
    for f in files:
        status = st.session_state["status_map"].get(f["id"], STATUS_PENDING)
        selections[f["id"]] = st.checkbox(f"{f['name']} ({status})", value=False, key=f"select_{f['id']}")

    force_rerun = st.checkbox("캐시 무시(재평가)", value=False)

    evaluate_selected = st.button("선택 평가")
    evaluate_all = st.button("전체 평가")
    load_history = st.button("히스토리/캐시 로드")

    rerun_file_id = st.session_state.get("rerun_file_id")
    if rerun_file_id:
        evaluate_selected = True
        force_rerun = True
        st.session_state["rerun_file_id"] = ""

    if evaluate_selected or evaluate_all:
        target_files = files if evaluate_all else [f for f in files if selections.get(f["id"])]
        if rerun_file_id:
            target_files = [f for f in files if f["id"] == rerun_file_id]
        if not target_files:
            st.warning("평가할 파일을 선택하세요.")
            return

        result_folder_id = ensure_results_folder(drive, folder_id)
        cache = CacheStore(drive, result_folder_id)
        cache.load()

        prompt_step1 = load_prompt(PROMPT_STEP1_PATH)
        prompt_step2 = load_prompt(PROMPT_STEP2_PATH)
        step1_hash = hash_prompt(prompt_step1)
        step2_hash = hash_prompt(prompt_step2)

        semaphore = threading.Semaphore(2)
        evaluator = Evaluator(api_key=api_key, semaphore=semaphore)

        results: List[Dict[str, Any]] = []
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
                except Exception:
                    results.append({"status": STATUS_FAILED, "file": {"id": "", "name": ""}})

        cache.save()

        st.success("평가 완료")
        for res in results:
            status = res.get("status")
            file_meta = res.get("file", {})
            file_id = file_meta.get("id", "")
            file_name = file_meta.get("name", "")
            if file_id:
                st.session_state["status_map"][file_id] = status
            if status == STATUS_SKIPPED:
                st.info(f"캐시 히트: {file_name}")
            elif status == STATUS_DONE:
                st.write(f"평가 완료: {file_name}")
            else:
                st.error(f"실패: {file_name}")
            cache_entry = res.get("cache", {})
            if cache_entry.get("report_file_url"):
                st.markdown(f"[리포트 열기]({cache_entry['report_file_url']})")
            if res.get("report_md"):
                st.session_state["last_report"] = res["report_md"]

        excel_bytes = cache_to_excel_bytes(cache, folder_id)
        st.download_button(
            label="엑셀 다운로드",
            data=excel_bytes,
            file_name=excel_filename(folder_id),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    if load_history and folder_id:
        result_folder_id = ensure_results_folder(drive, folder_id)
        cache = CacheStore(drive, result_folder_id)
        cache.load()
        items = list(cache.data.get("items", {}).values())
        if not items:
            st.info("히스토리가 없습니다.")
        else:
            st.subheader("히스토리")
            for entry in sorted(items, key=lambda x: x.get("timestamp", ""), reverse=True)[:20]:
                name = entry.get("file_name", "")
                url = entry.get("report_file_url", "")
                stamp = entry.get("timestamp", "")
                st.write(f"{stamp} - {name}")
                if url:
                    st.markdown(f"[리포트 열기]({url})")
                if st.button(f"재실행: {name}", key=f"rerun_{entry.get('file_id','')}"):
                    st.session_state["rerun_file_id"] = entry.get("file_id", "")
                    st.experimental_rerun()

            excel_bytes = cache_to_excel_bytes(cache, folder_id)
            st.download_button(
                label="엑셀 다운로드",
                data=excel_bytes,
                file_name=excel_filename(folder_id),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    st.subheader("결과 미리보기")
    if st.session_state.get("last_report"):
        st.markdown(st.session_state["last_report"])


if __name__ == "__main__":
    main()
