import pytest

from shared.utils.safe_bool_expression import (
    BoolExpressionEvaluationError,
    UnsafeBoolExpressionError,
    safe_eval_bool_expression,
    validate_bool_expression_syntax,
)


def test_safe_eval_bool_expression_supports_attribute_compare():
    variables = {"user": {"id": "alice"}, "ticket": {"requester": "bob"}}
    assert safe_eval_bool_expression("user.id != ticket.requester", variables=variables) is True


def test_safe_eval_bool_expression_supports_boolean_ops():
    variables = {"a": True, "b": False}
    assert safe_eval_bool_expression("a and not b", variables=variables) is True
    assert safe_eval_bool_expression("a and b", variables=variables) is False
    assert safe_eval_bool_expression("a or b", variables=variables) is True


def test_safe_eval_bool_expression_supports_subscript():
    variables = {"targets": [{"status": "OPEN"}]}
    assert safe_eval_bool_expression("targets[0].status == 'OPEN'", variables=variables) is True


def test_safe_eval_bool_expression_rejects_private_attribute_access():
    with pytest.raises(UnsafeBoolExpressionError):
        safe_eval_bool_expression("user.__class__", variables={"user": {"id": "alice"}})


def test_safe_eval_bool_expression_rejects_calls():
    with pytest.raises(UnsafeBoolExpressionError):
        safe_eval_bool_expression("__import__('os')", variables={})


def test_safe_eval_bool_expression_rejects_non_constant_subscript():
    with pytest.raises(UnsafeBoolExpressionError):
        safe_eval_bool_expression("targets[i] == 1", variables={"targets": [1], "i": 0})


def test_safe_eval_bool_expression_errors_on_unknown_identifier():
    with pytest.raises(BoolExpressionEvaluationError):
        safe_eval_bool_expression("missing == 1", variables={})


def test_safe_eval_bool_expression_errors_on_non_boolean_result():
    with pytest.raises(BoolExpressionEvaluationError):
        safe_eval_bool_expression("user.id", variables={"user": {"id": "alice"}})


def test_safe_eval_bool_expression_errors_on_not_non_boolean_operand():
    with pytest.raises(BoolExpressionEvaluationError):
        safe_eval_bool_expression("not user.id", variables={"user": {"id": "alice"}})


def test_validate_bool_expression_syntax_accepts_safe_expressions():
    validate_bool_expression_syntax("user.id != 'alice'")


def test_validate_bool_expression_syntax_rejects_calls():
    with pytest.raises(UnsafeBoolExpressionError):
        validate_bool_expression_syntax("__import__('os')")
