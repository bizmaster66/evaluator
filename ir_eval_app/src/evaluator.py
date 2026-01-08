from __future__ import annotations

import json
import re
import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional

from google import genai

from .config import MODEL_NAME


@dataclass
class EvalResult:
    step1: Dict[str, Any]
    step2: Optional[Dict[str, Any]]
    raw_step1: str
    raw_step2: str


class Evaluator:
    def __init__(self, api_key: str, semaphore: threading.Semaphore):
        self.client = genai.Client(api_key=api_key)
        self.semaphore = semaphore

    def evaluate_step1(self, content: str, prompt_step1: str, schema_hint_step1: str) -> Dict[str, Any]:
        step1_prompt = self._build_prompt(prompt_step1, schema_hint_step1, content)
        step1_text = self._call_model(step1_prompt)
        return json_load(step1_text)

    def evaluate_step2(
        self,
        content: str,
        prompt_step2: str,
        schema_hint_step2: str,
        step1_json: Dict[str, Any],
    ) -> Dict[str, Any]:
        step2_prompt = self._build_prompt_with_step1(prompt_step2, schema_hint_step2, content, step1_json)
        step2_text = self._call_model(step2_prompt)
        return json_load(step2_text)

    def _build_prompt(self, prompt: str, schema_hint: str, content: str) -> str:
        return (
            f"{prompt}\n\n"
            f"JSON schema hints:\n{schema_hint}\n\n"
            f"IR full text:\n{content}\n\n"
            f"Return JSON only."
        )

    def _build_prompt_with_step1(self, prompt: str, schema_hint: str, content: str, step1_json: Dict[str, Any]) -> str:
        step1_block = json.dumps(step1_json, ensure_ascii=True)
        return (
            f"{prompt}\n\n"
            f"JSON schema hints:\n{schema_hint}\n\n"
            f"STEP1 JSON:\n{step1_block}\n\n"
            f"IR full text:\n{content}\n\n"
            f"Return JSON only."
        )

    def _call_model(self, prompt: str) -> str:
        with self.semaphore:
            response = self.client.models.generate_content(
                model=MODEL_NAME,
                contents=prompt,
                config={
                    "response_mime_type": "application/json",
                    "temperature": 0,
                },
            )
        return response.text or "{}"


def _extract_json_object(text: str) -> str:
    """Try to recover a JSON object from noisy model output."""
    if not text:
        return "{}"
    s = text.strip()

    # Common wrappers
    if s.startswith("```") and s.endswith("```"):
        s = re.sub(r"^```[a-zA-Z0-9_-]*\n", "", s)
        s = re.sub(r"\n```$", "", s).strip()

    # Fast path
    if s.startswith("{") and s.endswith("}"):
        return s

    # Find first top-level {...}
    start = s.find("{")
    end = s.rfind("}")
    if start != -1 and end != -1 and end > start:
        return s[start : end + 1].strip()

    return s


def json_load(text: str) -> Dict[str, Any]:
    s = _extract_json_object(text)
    try:
        data = json.loads(s)
    except json.JSONDecodeError as exc:
        raise ValueError("Model did not return valid JSON") from exc
    if not isinstance(data, dict):
        raise ValueError("Model returned non-object JSON")
    return data
