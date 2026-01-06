from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

MODEL_NAME = "gemini-2.5-flash"
RESULTS_FOLDER_NAME = "IR_EVAL_RESULTS"
CACHE_FILE_NAME = "cache_index.json"
LOG_SHEET_NAME = "IR_EVAL_LOG"

PROMPT_STEP1_PATH = Path(__file__).resolve().parents[1] / "prompts" / "IRDECK_evaluator_step1.md"
PROMPT_STEP2_PATH = Path(__file__).resolve().parents[1] / "prompts" / "IRdeck_evaluator_step2.md"


def load_prompt(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def hash_prompt(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def md5_text(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()  # nosec - content hash only


def to_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"))
