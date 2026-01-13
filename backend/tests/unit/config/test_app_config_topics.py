from __future__ import annotations

from shared.config.app_config import AppConfig


def test_get_all_topics_includes_command_dlqs() -> None:
    topics = AppConfig.get_all_topics()

    assert AppConfig.INSTANCE_COMMANDS_DLQ_TOPIC in topics
    assert AppConfig.ONTOLOGY_COMMANDS_DLQ_TOPIC in topics
    assert AppConfig.ACTION_COMMANDS_DLQ_TOPIC in topics

