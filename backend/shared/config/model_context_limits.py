"""
Model-specific context window configurations.

Provides dynamic context limits based on LLM model capabilities,
enabling optimal use of modern LLM context windows.
"""

from dataclasses import dataclass
from typing import Dict, Optional, Any


@dataclass(frozen=True)
class ModelContextConfig:
    """Configuration for a specific LLM model's context capabilities."""

    max_context_tokens: int
    max_output_tokens: int
    chars_per_token_estimate: float = 4.0  # Conservative estimate

    @property
    def safe_prompt_chars(self) -> int:
        """
        Calculate safe prompt character limit.

        Uses 70% of context window for prompt, leaving 30% for output.
        This provides buffer for response generation.
        """
        usable_tokens = int(self.max_context_tokens * 0.7)
        return int(usable_tokens * self.chars_per_token_estimate)


# Hardcoded fallback for known models (used when registry unavailable)
MODEL_CONTEXT_DEFAULTS: Dict[str, ModelContextConfig] = {
    # OpenAI GPT-4 family
    "gpt-4": ModelContextConfig(max_context_tokens=8192, max_output_tokens=4096),
    "gpt-4-32k": ModelContextConfig(max_context_tokens=32768, max_output_tokens=4096),
    "gpt-4-turbo": ModelContextConfig(max_context_tokens=128000, max_output_tokens=4096),
    "gpt-4o": ModelContextConfig(max_context_tokens=128000, max_output_tokens=16384),
    "gpt-4o-mini": ModelContextConfig(max_context_tokens=128000, max_output_tokens=16384),
    # OpenAI GPT-5 (estimated based on trends)
    "gpt-5": ModelContextConfig(max_context_tokens=200000, max_output_tokens=32768),
    # OpenAI GPT-3.5
    "gpt-3.5": ModelContextConfig(max_context_tokens=4096, max_output_tokens=4096),
    "gpt-3.5-turbo": ModelContextConfig(max_context_tokens=16385, max_output_tokens=4096),
    # Anthropic Claude 3 family
    "claude-3-opus": ModelContextConfig(max_context_tokens=200000, max_output_tokens=4096),
    "claude-3-sonnet": ModelContextConfig(max_context_tokens=200000, max_output_tokens=4096),
    "claude-3-haiku": ModelContextConfig(max_context_tokens=200000, max_output_tokens=4096),
    "claude-3.5-sonnet": ModelContextConfig(max_context_tokens=200000, max_output_tokens=8192),
    "claude-3.5-haiku": ModelContextConfig(max_context_tokens=200000, max_output_tokens=8192),
    # Anthropic Claude 2
    "claude-2": ModelContextConfig(max_context_tokens=100000, max_output_tokens=4096),
    # Google Gemini
    "gemini-pro": ModelContextConfig(max_context_tokens=32000, max_output_tokens=8192),
    "gemini-1.5-pro": ModelContextConfig(max_context_tokens=1000000, max_output_tokens=8192),
    "gemini-1.5-flash": ModelContextConfig(max_context_tokens=1000000, max_output_tokens=8192),
    "gemini-2": ModelContextConfig(max_context_tokens=1000000, max_output_tokens=8192),
}

# Conservative default for unknown models
DEFAULT_CONFIG = ModelContextConfig(
    max_context_tokens=8000,
    max_output_tokens=2000,
    chars_per_token_estimate=4.0,
)


def get_model_context_config(
    model_id: str,
    registry_record: Optional[Any] = None,
) -> ModelContextConfig:
    """
    Resolve context config with priority:
    1. AgentModelRegistry record (database) - if provided
    2. Hardcoded defaults (MODEL_CONTEXT_DEFAULTS)
    3. Conservative fallback (DEFAULT_CONFIG)

    Args:
        model_id: The model identifier (e.g., "gpt-4o", "claude-3-opus")
        registry_record: Optional AgentModelRecord from database

    Returns:
        ModelContextConfig with appropriate context limits
    """
    # Priority 1: Database registry (if record provided with context info)
    if registry_record is not None:
        max_ctx = getattr(registry_record, "max_context_tokens", None)
        max_out = getattr(registry_record, "max_output_tokens", None)
        if max_ctx and max_ctx > 0:
            return ModelContextConfig(
                max_context_tokens=int(max_ctx),
                max_output_tokens=int(max_out) if max_out else 4096,
            )

    # Priority 2: Hardcoded lookup (normalize model name)
    model_lower = (model_id or "").strip().lower()

    # Try exact match first
    if model_lower in MODEL_CONTEXT_DEFAULTS:
        return MODEL_CONTEXT_DEFAULTS[model_lower]

    # Try prefix match (e.g., "gpt-4o-2024-01-01" matches "gpt-4o")
    for prefix, config in MODEL_CONTEXT_DEFAULTS.items():
        if model_lower.startswith(prefix):
            return config

    # Priority 3: Conservative default
    return DEFAULT_CONFIG
