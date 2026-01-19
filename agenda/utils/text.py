from __future__ import annotations

import re
import unicodedata


def normalize_name(value: str) -> str:
    if value is None:
        return ""
    text = str(value).strip().upper()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"[^A-Z0-9\\s]", " ", text)
    text = re.sub(r"\\s+", " ", text)
    return text.strip()
