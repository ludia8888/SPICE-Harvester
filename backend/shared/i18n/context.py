from __future__ import annotations

from contextvars import ContextVar
from typing import Optional

from shared.utils.language import normalize_language

_LANGUAGE: ContextVar[str] = ContextVar("spice_language", default="ko")


def set_language(lang: Optional[str]) -> object:
    return _LANGUAGE.set(normalize_language(lang))


def reset_language(token: object) -> None:
    _LANGUAGE.reset(token)


def get_language() -> str:
    return _LANGUAGE.get()

