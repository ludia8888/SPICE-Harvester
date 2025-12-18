"""
I18N / localized-text primitives (model layer).

This module is intentionally dependency-light (no FastAPI imports) so it can be used
by shared Pydantic models and workers.
"""

from __future__ import annotations

from typing import Dict, Union

# Backward compatible:
# - Clients can send a plain string
# - Or a language map like {"en": "...", "ko": "..."}
LocalizedText = Union[str, Dict[str, str]]

