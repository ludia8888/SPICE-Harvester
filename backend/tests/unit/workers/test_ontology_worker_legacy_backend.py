from __future__ import annotations

import pytest
from unittest.mock import AsyncMock

from ontology_worker.main import OntologyWorker
from shared.models.commands import CommandType


@pytest.mark.unit
@pytest.mark.asyncio
async def test_process_command_allows_database_command_without_adapter_in_postgres_profile() -> None:
    worker = OntologyWorker()
    worker.ontology_resource_backend = "postgres"
    worker.handle_create_database = AsyncMock(return_value=None)  # type: ignore[method-assign]

    await worker.process_command(
        {
            "command_type": CommandType.CREATE_DATABASE,
            "payload": {"database_name": "demo"},
        }
    )

    worker.handle_create_database.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_process_command_allows_ontology_command_without_adapter_in_postgres_profile() -> None:
    worker = OntologyWorker()
    worker.ontology_resource_backend = "postgres"
    worker.handle_create_ontology = AsyncMock(return_value=None)  # type: ignore[method-assign]

    await worker.process_command(
        {
            "command_type": CommandType.CREATE_ONTOLOGY_CLASS,
            "payload": {"db_name": "demo", "class_id": "Ticket", "branch": "main"},
        }
    )

    worker.handle_create_ontology.assert_awaited_once()
