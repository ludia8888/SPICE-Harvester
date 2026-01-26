from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from shared.config.app_config import AppConfig
from shared.services.storage.lakefs_storage_service import LakeFSStorageService
from shared.services.storage.storage_service import StorageService
from shared.utils.writeback_lifecycle import DEFAULT_LIFECYCLE_ID, derive_lifecycle_id
from shared.utils.writeback_paths import (
    queue_entry_prefix,
    ref_key,
    snapshot_latest_pointer_key,
    snapshot_manifest_key,
    snapshot_object_key,
    writeback_patchset_key,
)


def _parse_queue_entry_seq(key: str) -> Optional[int]:
    name = str(key or "").rsplit("/", 1)[-1]
    if not name.endswith(".json"):
        return None
    stem = name[:-5]
    head = stem.split("_", 1)[0]
    try:
        return int(head)
    except (TypeError, ValueError):
        return None


def _coerce_object_type(resource_rid: str, *, fallback: str) -> str:
    rid = str(resource_rid or "").strip()
    if ":" in rid:
        rid = rid.split(":", 1)[1]
    if "@" in rid:
        rid = rid.split("@", 1)[0]
    return rid.strip() or fallback


def _apply_changes_to_payload(payload: Dict[str, Any], changes: Dict[str, Any]) -> bool:
    if bool(changes.get("delete")):
        return True

    set_ops = changes.get("set") if isinstance(changes.get("set"), dict) else {}
    unset_ops = changes.get("unset") if isinstance(changes.get("unset"), list) else []

    for key, value in (set_ops or {}).items():
        if isinstance(key, str) and key:
            payload[key] = value
    for key in unset_ops or []:
        if isinstance(key, str) and key:
            payload.pop(key, None)

    # Link ops are supported in the patchset shape but currently best-effort.
    # Accept either {"field": "...", "value": "..."} or "field:value".
    for op_key, add in (("link_add", True), ("link_remove", False)):
        ops = changes.get(op_key)
        if not isinstance(ops, list):
            continue
        for item in ops:
            field = None
            target = None
            if isinstance(item, dict):
                field = item.get("field") or item.get("predicate") or item.get("name")
                target = item.get("value") or item.get("to") or item.get("target")
                if (field is None or target is None) and len(item) == 1:
                    k, v = next(iter(item.items()))
                    field = k
                    target = v
            elif isinstance(item, str):
                raw = item.strip()
                if ":" in raw:
                    field, target = raw.split(":", 1)
            field_str = str(field or "").strip()
            target_str = str(target or "").strip()
            if not field_str or not target_str:
                continue
            existing = payload.get(field_str)
            if isinstance(existing, list):
                values = [str(v) for v in existing if v is not None]
            elif existing is None:
                values = []
            else:
                values = [str(existing)]
            if add:
                if target_str not in values:
                    values.append(target_str)
            else:
                values = [v for v in values if v != target_str]
            payload[field_str] = values

    return False


@dataclass(frozen=True)
class WritebackMergedInstance:
    document: Dict[str, Any]
    writeback_edits_present: bool
    lifecycle_id: str
    last_action_applied_seq: Optional[int]
    last_patchset_commit_id: Optional[str]
    last_ontology_commit_id: Optional[str]
    overlay_tombstone: bool


class WritebackMergeService:
    """
    Authoritative server-side merge path for Action writeback.

    This is used when ES overlay is unavailable (overlay_status=DEGRADED).
    """

    def __init__(
        self,
        *,
        base_storage: StorageService,
        lakefs_storage: LakeFSStorageService,
    ) -> None:
        self._base_storage = base_storage
        self._lakefs_storage = lakefs_storage

    async def merge_instance(
        self,
        *,
        db_name: str,
        base_branch: str,
        overlay_branch: str,
        class_id: str,
        instance_id: str,
        writeback_repo: str,
        writeback_branch: str,
    ) -> WritebackMergedInstance:
        prefix = f"{db_name}/{base_branch}/{class_id}/{instance_id}/"
        command_files = await self._base_storage.list_command_files(
            bucket=AppConfig.INSTANCE_BUCKET,
            prefix=prefix,
        )
        if not command_files:
            raise FileNotFoundError("base_instance_not_found")
        base_state = await self._base_storage.replay_instance_state(
            bucket=AppConfig.INSTANCE_BUCKET,
            command_files=command_files,
        )
        if not isinstance(base_state, dict) or not base_state:
            raise RuntimeError("base_instance_state_unavailable")
        meta = base_state.get("_metadata") if isinstance(base_state.get("_metadata"), dict) else {}
        if meta.get("deleted") is True:
            raise FileNotFoundError("base_instance_deleted")

        lifecycle_id = derive_lifecycle_id(base_state) or DEFAULT_LIFECYCLE_ID

        snapshot_high_watermark: Optional[int] = None
        snapshot_used = False
        try:
            latest_ptr = await self._lakefs_storage.load_json(
                bucket=writeback_repo,
                key=ref_key(writeback_branch, snapshot_latest_pointer_key()),
            )
        except FileNotFoundError:
            latest_ptr = None
        snapshot_id = ""
        if isinstance(latest_ptr, dict):
            snapshot_id = str(latest_ptr.get("snapshot_id") or latest_ptr.get("id") or "").strip()
        if snapshot_id:
            try:
                manifest = await self._lakefs_storage.load_json(
                    bucket=writeback_repo,
                    key=ref_key(writeback_branch, snapshot_manifest_key(snapshot_id)),
                )
            except FileNotFoundError:
                manifest = None
            if isinstance(manifest, dict):
                try:
                    snapshot_high_watermark = int(manifest.get("queue_high_watermark"))
                except (TypeError, ValueError):
                    snapshot_high_watermark = None
            try:
                snapshot_obj = await self._lakefs_storage.load_json(
                    bucket=writeback_repo,
                    key=ref_key(
                        writeback_branch,
                        snapshot_object_key(
                            snapshot_id=snapshot_id,
                            object_type=class_id,
                            instance_id=instance_id,
                            lifecycle_id=lifecycle_id,
                        ),
                    ),
                )
            except FileNotFoundError:
                snapshot_obj = None
            if isinstance(snapshot_obj, dict) and snapshot_obj:
                base_state = snapshot_obj
                snapshot_used = True

        queue_prefix = ref_key(
            writeback_branch,
            queue_entry_prefix(
                object_type=class_id,
                instance_id=instance_id,
                lifecycle_id=lifecycle_id,
            ),
        )

        queue_keys: list[tuple[int, str]] = []
        async for obj in self._lakefs_storage.iter_objects(bucket=writeback_repo, prefix=queue_prefix):
            key = str(obj.get("Key") or "")
            if not key or not key.endswith(".json"):
                continue
            seq = _parse_queue_entry_seq(key)
            if seq is None:
                continue
            if snapshot_high_watermark is not None and seq <= snapshot_high_watermark:
                continue
            queue_keys.append((seq, key))
        queue_keys.sort(key=lambda item: (item[0], item[1]))

        effective = {
            "instance_id": instance_id,
            "class_id": class_id,
            "db_name": db_name,
            "branch": overlay_branch,
            "overlay_tombstone": False,
            "lifecycle_id": lifecycle_id,
            "data": dict(base_state),
        }

        if not queue_keys:
            return WritebackMergedInstance(
                document=effective,
                writeback_edits_present=bool(snapshot_used),
                lifecycle_id=lifecycle_id,
                last_action_applied_seq=None,
                last_patchset_commit_id=None,
                last_ontology_commit_id=None,
                overlay_tombstone=False,
            )

        last_seq: Optional[int] = None
        last_patchset_commit_id: Optional[str] = None
        last_ontology_commit_id: Optional[str] = None
        overlay_tombstone = False

        for seq, queue_key in queue_keys:
            entry = await self._lakefs_storage.load_json(bucket=writeback_repo, key=queue_key)
            if not isinstance(entry, dict):
                continue
            action_log_id = str(entry.get("action_log_id") or "").strip()
            patchset_commit_id = str(entry.get("patchset_commit_id") or "").strip()
            if not action_log_id or not patchset_commit_id:
                continue

            patchset = await self._lakefs_storage.load_json(
                bucket=writeback_repo,
                key=ref_key(patchset_commit_id, writeback_patchset_key(action_log_id)),
            )
            targets = patchset.get("targets") if isinstance(patchset, dict) else None
            if not isinstance(targets, list):
                targets = []

            for target in targets:
                if not isinstance(target, dict):
                    continue
                if str(target.get("instance_id") or "").strip() != instance_id:
                    continue
                if str(target.get("lifecycle_id") or "").strip() != lifecycle_id:
                    continue
                resource_rid = str(target.get("resource_rid") or "")
                object_type = _coerce_object_type(resource_rid, fallback=class_id)
                if object_type != class_id:
                    continue

                changes = None
                if isinstance(target.get("applied_changes"), dict):
                    changes = target.get("applied_changes")
                elif isinstance(target.get("changes"), dict):
                    changes = target.get("changes")
                else:
                    changes = {}

                tombstone = _apply_changes_to_payload(effective["data"], changes)
                last_seq = int(seq)
                last_patchset_commit_id = patchset_commit_id
                last_ontology_commit_id = str(patchset.get("ontology_commit_id") or "").strip() or last_ontology_commit_id
                if tombstone:
                    overlay_tombstone = True
                    effective["overlay_tombstone"] = True
                    effective["data"] = {
                        "instance_id": instance_id,
                        "class_id": class_id,
                        "db_name": db_name,
                        "lifecycle_id": lifecycle_id,
                        "_metadata": {
                            "deleted": True,
                            "deleted_via": "writeback",
                            "deleted_at_seq": int(seq),
                            "deleted_patchset_commit_id": patchset_commit_id,
                        },
                    }
                    break

            if overlay_tombstone:
                break

        return WritebackMergedInstance(
            document=effective,
            writeback_edits_present=True,
            lifecycle_id=lifecycle_id,
            last_action_applied_seq=last_seq,
            last_patchset_commit_id=last_patchset_commit_id,
            last_ontology_commit_id=last_ontology_commit_id,
            overlay_tombstone=overlay_tombstone,
        )
