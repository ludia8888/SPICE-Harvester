from __future__ import annotations

from shared.services.kafka.retry_classifier import (
    ACTION_COMMAND_RETRY_PROFILE,
    RetryPolicyProfile,
    classify_retryable_by_markers,
    classify_retryable_with_profile,
    contains_marker,
    create_retry_policy_profile,
    normalize_error_message,
)


def test_normalize_error_message_lowercases() -> None:
    message = normalize_error_message(RuntimeError("API ERROR: 400 Bad Request"))
    assert message == "api error: 400 bad request"


def test_contains_marker_trims_and_matches_case_insensitive() -> None:
    assert contains_marker("validation failure", (" Validation ",))
    assert not contains_marker("transient timeout", ("bad request",))


def test_classify_retryable_by_markers_priority_and_default() -> None:
    bad_request = RuntimeError("Bad Request payload")
    timeout = RuntimeError("Timeout while contacting storage")
    unknown = RuntimeError("unexpected failure")

    assert (
        classify_retryable_by_markers(
            bad_request,
            non_retryable_markers=("bad request",),
            retryable_markers=("timeout",),
            default_retryable=True,
        )
        is False
    )
    assert (
        classify_retryable_by_markers(
            timeout,
            non_retryable_markers=("bad request",),
            retryable_markers=("timeout",),
            default_retryable=False,
        )
        is True
    )
    assert (
        classify_retryable_by_markers(
            unknown,
            non_retryable_markers=("bad request",),
            retryable_markers=("timeout",),
            default_retryable=True,
        )
        is True
    )


def test_create_retry_policy_profile_normalizes_markers() -> None:
    profile = create_retry_policy_profile(
        non_retryable_markers=(" Bad Request ", "", "INVALID"),
        retryable_markers=(" TIMEOUT ",),
        default_retryable=False,
    )
    assert profile == RetryPolicyProfile(
        non_retryable_markers=("bad request", "invalid"),
        retryable_markers=("timeout",),
        default_retryable=False,
    )


def test_classify_retryable_with_profile_uses_predefined_profile() -> None:
    assert classify_retryable_with_profile(RuntimeError("validation failed"), ACTION_COMMAND_RETRY_PROFILE) is False
    assert classify_retryable_with_profile(RuntimeError("timeout while connecting"), ACTION_COMMAND_RETRY_PROFILE) is True
