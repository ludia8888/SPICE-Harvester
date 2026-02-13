from __future__ import annotations

import logging
import warnings


def test_base_response_emits_deprecation_once(monkeypatch, caplog) -> None:
    from shared.models import common as common_models

    monkeypatch.setattr(common_models, "_base_response_deprecation_emitted", False)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        with caplog.at_level(logging.WARNING, logger="shared.models.common"):
            common_models.BaseResponse.success("ok")
            common_models.BaseResponse.success("ok-again")

    dep_warnings = [item for item in caught if issubclass(item.category, DeprecationWarning)]
    assert len(dep_warnings) == 1

    deprecation_logs = [
        rec for rec in caplog.records if "[deprecation][base_response_alias]" in rec.message
    ]
    assert len(deprecation_logs) == 1
