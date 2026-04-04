from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel, Field


class ApplyActionRequestOptionsV2(BaseModel):
    mode: str | None = None
    return_edits: str | None = Field(default=None, alias="returnEdits")


class ApplyActionRequestV2(BaseModel):
    options: ApplyActionRequestOptionsV2 | None = None
    parameters: Dict[str, Any] = Field(default_factory=dict)


class BatchApplyActionRequestItemV2(BaseModel):
    parameters: Dict[str, Any] = Field(default_factory=dict)


class BatchApplyActionRequestOptionsV2(BaseModel):
    return_edits: str | None = Field(default=None, alias="returnEdits")


class BatchApplyActionRequestV2(BaseModel):
    options: BatchApplyActionRequestOptionsV2 | None = None
    requests: list[BatchApplyActionRequestItemV2] = Field(default_factory=list, min_length=1, max_length=20)


class ExecuteQueryRequestV2(BaseModel):
    parameters: Dict[str, Any] = Field(default_factory=dict)
    options: Dict[str, Any] | None = None


class ObjectTypeContractCreateRequestV2(BaseModel):
    apiName: str = Field(..., min_length=1)
    status: str | None = None
    primaryKey: str | None = None
    titleProperty: str | None = None
    pkSpec: Dict[str, Any] | None = None
    backingSource: Dict[str, Any] | None = None
    backingSources: list[Dict[str, Any]] | None = None
    backingDatasetId: str | None = None
    backingDatasourceId: str | None = None
    backingDatasourceVersionId: str | None = None
    datasetVersionId: str | None = None
    schemaHash: str | None = None
    mappingSpecId: str | None = None
    mappingSpecVersion: int | None = None
    autoGenerateMapping: bool | None = None
    metadata: Dict[str, Any] | None = None


class ObjectTypeContractUpdateRequestV2(BaseModel):
    status: str | None = None
    primaryKey: str | None = None
    titleProperty: str | None = None
    pkSpec: Dict[str, Any] | None = None
    backingSource: Dict[str, Any] | None = None
    backingSources: list[Dict[str, Any]] | None = None
    backingDatasetId: str | None = None
    backingDatasourceId: str | None = None
    backingDatasourceVersionId: str | None = None
    datasetVersionId: str | None = None
    schemaHash: str | None = None
    mappingSpecId: str | None = None
    mappingSpecVersion: int | None = None
    metadata: Dict[str, Any] | None = None
    migration: Dict[str, Any] | None = None
