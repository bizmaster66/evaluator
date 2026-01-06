from __future__ import annotations

import hashlib
from typing import Iterable


def hash_cache_key(parts: Iterable[str]) -> str:
    joined = "::".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()
