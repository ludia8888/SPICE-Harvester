"""
Shared i18n helpers (EN/KR).

Design goals:
- No external translation APIs (deterministic)
- Request-scoped language via ContextVar (set by middleware)
- Call-site can provide both EN/KR strings, or rely on generic fallbacks
"""

from .context import get_language, reset_language, set_language
from .translator import localize_free_text, m

__all__ = [
    "get_language",
    "set_language",
    "reset_language",
    "m",
    "localize_free_text",
]

