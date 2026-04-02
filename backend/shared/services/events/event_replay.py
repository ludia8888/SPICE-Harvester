#!/usr/bin/env python3
"""
Event Replay Service
THINK ULTRA³ - S3 based deterministic replay

Replays events from S3 to reconstruct state at any point in time
NO MOCKS, REAL EVENT SOURCING
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import boto3
from botocore.client import Config
import hashlib

class EventReplayService:
    """
    Deterministic event replay from S3 storage
    Reconstructs complete state history from immutable event log
    """
    
    def __init__(
        self,
        s3_endpoint: str = "http://localhost:9000",
        s3_access_key: str = "minioadmin",
        s3_secret_key: str = "minioadmin123",
        bucket_name: str = "instance-events",
        *,
        s3_client: Any | None = None,
    ):
        self.s3_client = s3_client or boto3.client(
            "s3",
            endpoint_url=s3_endpoint,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        self.bucket_name = bucket_name
        self._supported_command_types = {
            "CREATE_INSTANCE",
            "BULK_CREATE_INSTANCES",
            "UPDATE_INSTANCE",
            "DELETE_INSTANCE",
        }

    def _list_all_objects(self, *, prefix: str) -> List[Dict[str, Any]]:
        objects: List[Dict[str, Any]] = []
        continuation_token: Optional[str] = None

        while True:
            params: Dict[str, Any] = {
                "Bucket": self.bucket_name,
                "Prefix": prefix,
                "MaxKeys": 1000,
            }
            if continuation_token:
                params["ContinuationToken"] = continuation_token
            response = self.s3_client.list_objects_v2(**params)
            contents = response.get("Contents", []) or []
            objects.extend(obj for obj in contents if isinstance(obj, dict))
            if not response.get("IsTruncated"):
                break
            continuation_token = response.get("NextContinuationToken")

        return objects

    def _load_json_object(self, *, key: str) -> Dict[str, Any]:
        obj_data = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
        body = obj_data.get("Body")
        if body is None:
            return {}
        try:
            raw = body.read()
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                close()
        return json.loads(raw.decode("utf-8"))

    @staticmethod
    def _extract_aggregate_id_from_index_key(key: str) -> Optional[str]:
        parts = str(key or "").split("/")
        if len(parts) < 5 or parts[0] != "indexes" or parts[1] != "by-aggregate":
            return None
        return parts[3] or None

    def _list_current_layout_index_entries(
        self,
        *,
        class_id: str,
        aggregate_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        prefix = f"indexes/by-aggregate/{class_id}/"
        if aggregate_id:
            prefix = f"{prefix}{aggregate_id}/"

        entries: List[Dict[str, Any]] = []
        for obj in self._list_all_objects(prefix=prefix):
            index_key = str(obj.get("Key") or "")
            entry = self._load_json_object(key=index_key)
            s3_key = str(entry.get("s3_key") or "").strip()
            if not s3_key:
                continue
            entries.append(
                {
                    "index_key": index_key,
                    "aggregate_id": aggregate_id or self._extract_aggregate_id_from_index_key(index_key),
                    "s3_key": s3_key,
                    "last_modified": obj.get("LastModified"),
                    "sequence_number": entry.get("sequence_number", 0),
                    "event_type": entry.get("event_type"),
                }
            )
        return entries

    @staticmethod
    def _is_event_envelope(payload: Dict[str, Any]) -> bool:
        return all(
            key in payload
            for key in ("event_id", "event_type", "aggregate_type", "aggregate_id", "occurred_at")
        )

    def _normalize_event_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("Replay payload must be a JSON object")

        if not self._is_event_envelope(payload):
            return payload

        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        event_type = str(payload.get("event_type") or "").strip()
        command_type = (
            metadata.get("command_type")
            or payload.get("command_type")
            or event_type
        )
        if isinstance(command_type, str) and command_type.endswith("_REQUESTED"):
            command_type = command_type[: -len("_REQUESTED")]
        command_type = str(command_type or "").strip().upper()

        if command_type not in self._supported_command_types:
            raise ValueError(
                "Unsupported current EventStore event layout for replay: "
                f"missing supported command_type in {payload.get('event_id')}"
            )

        payload_data = data.get("payload")
        if not isinstance(payload_data, dict):
            payload_data = {}

        return {
            "command_type": command_type,
            "command_id": metadata.get("command_id") or payload.get("event_id"),
            "sequence_number": payload.get("sequence_number", 0),
            "created_at": data.get("created_at") or payload.get("occurred_at"),
            "timestamp": payload.get("occurred_at"),
            "created_by": payload.get("actor"),
            "payload": payload_data,
            "db_name": data.get("db_name"),
        }

    @staticmethod
    def _coerce_timestamp(value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str) and value.strip():
            candidate = value.strip().replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(candidate)
            except ValueError:
                return None
            return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        return None

    @staticmethod
    def _event_identity(event: Dict[str, Any]) -> str:
        content = event.get("content") if isinstance(event.get("content"), dict) else {}
        sequence = int(content.get("sequence_number") or 0)
        command_id = str(content.get("command_id") or "").strip()
        command_type = str(content.get("command_type") or "").strip()
        if sequence and command_id:
            return f"seq:{sequence}:cmd:{command_id}:{command_type}"
        if command_id:
            return f"cmd:{command_id}:{command_type}"
        return str(event.get("key") or "")

    def _event_matches_db_name(self, payload: Dict[str, Any], *, db_name: str) -> bool:
        normalized = self._normalize_event_payload(payload)
        event_db_name = normalized.get("db_name")
        if not event_db_name:
            return True
        return str(event_db_name) == str(db_name)

    async def _list_all_objects_async(self, *, prefix: str) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self._list_all_objects, prefix=prefix)

    async def _load_json_object_async(self, *, key: str) -> Dict[str, Any]:
        return await asyncio.to_thread(self._load_json_object, key=key)

    def _load_current_layout_events(
        self,
        *,
        db_name: str,
        class_id: str,
        aggregate_id: str,
    ) -> List[Dict[str, Any]]:
        entries = self._list_current_layout_index_entries(class_id=class_id, aggregate_id=aggregate_id)
        if not entries:
            return []

        events: List[Dict[str, Any]] = []
        for entry in entries:
            raw_payload = self._load_json_object(key=entry["s3_key"])
            if not self._event_matches_db_name(raw_payload, db_name=db_name):
                continue
            normalized = self._normalize_event_payload(raw_payload)
            events.append(
                {
                    "key": entry["s3_key"],
                    "timestamp": self._coerce_timestamp(normalized.get("timestamp")) or entry.get("last_modified"),
                    "content": normalized,
                }
            )
        return events

    async def _load_current_layout_events_from_objects(
        self,
        *,
        db_name: str,
        class_id: str,
        aggregate_id: str,
    ) -> List[Dict[str, Any]]:
        objects = await self._list_all_objects_async(prefix="events/")
        events: List[Dict[str, Any]] = []
        aggregate_path = f"/{class_id}/{aggregate_id}/"
        for obj in objects:
            key = str(obj.get("Key") or "")
            if aggregate_path not in key:
                continue
            raw_payload = await self._load_json_object_async(key=key)
            if not self._event_matches_db_name(raw_payload, db_name=db_name):
                continue
            normalized = self._normalize_event_payload(raw_payload)
            events.append(
                {
                    "key": key,
                    "timestamp": self._coerce_timestamp(normalized.get("timestamp")) or obj.get("LastModified"),
                    "content": normalized,
                }
            )
        return events

    def _load_legacy_events(
        self,
        *,
        db_name: str,
        class_id: str,
        aggregate_id: str,
    ) -> List[Dict[str, Any]]:
        prefix = f"{db_name}/{class_id}/{aggregate_id}/"
        objects = self._list_all_objects(prefix=prefix)
        events: List[Dict[str, Any]] = []
        for obj in objects:
            content = self._load_json_object(key=obj["Key"])
            events.append(
                {
                    "key": obj["Key"],
                    "timestamp": self._coerce_timestamp(content.get("timestamp") or content.get("created_at")) or obj["LastModified"],
                    "content": content,
                }
            )
        return events

    @classmethod
    def _merge_event_collections(cls, *collections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        merged: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for collection in collections:
            for event in collection:
                identity = cls._event_identity(event)
                if identity in seen:
                    continue
                seen.add(identity)
                merged.append(event)
        merged.sort(
            key=lambda x: (
                x["content"].get("sequence_number", 0),
                cls._coerce_timestamp(x.get("timestamp")) or datetime.min.replace(tzinfo=timezone.utc),
            )
        )
        return merged
        
    async def replay_aggregate(
        self, 
        db_name: str, 
        class_id: str, 
        aggregate_id: str,
        up_to_sequence: Optional[int] = None,
        up_to_timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Replay all events for a specific aggregate
        
        Args:
            db_name: Database name
            class_id: Class/Type ID
            aggregate_id: Aggregate instance ID
            up_to_sequence: Replay up to this sequence number (inclusive)
            up_to_timestamp: Replay up to this timestamp
            
        Returns:
            Final state after replay
        """
        print(f"🔄 Replaying aggregate: {aggregate_id}")

        current_layout_events, current_layout_scan_events, legacy_events = await asyncio.gather(
            asyncio.to_thread(
                self._load_current_layout_events,
                db_name=db_name,
                class_id=class_id,
                aggregate_id=aggregate_id,
            ),
            self._load_current_layout_events_from_objects(
                db_name=db_name,
                class_id=class_id,
                aggregate_id=aggregate_id,
            ),
            asyncio.to_thread(
                self._load_legacy_events,
                db_name=db_name,
                class_id=class_id,
                aggregate_id=aggregate_id,
            ),
        )

        events = self._merge_event_collections(current_layout_events, current_layout_scan_events, legacy_events)

        if not events:
            return {
                "aggregate_id": aggregate_id,
                "status": "NOT_FOUND",
                "event_count": 0,
                "final_state": None
            }
        
        # Sort by sequence number if available, else by timestamp
        # Apply filters
        filtered_events = []
        for event in events:
            content = event['content']
            
            # Check sequence filter
            if up_to_sequence is not None:
                seq = content.get('sequence_number', 0)
                if seq > up_to_sequence:
                    continue
            
            # Check timestamp filter
            if up_to_timestamp is not None:
                event_ts = self._coerce_timestamp(event["content"].get("timestamp")) or self._coerce_timestamp(event.get("timestamp"))
                if event_ts is None:
                    continue
                if event_ts > up_to_timestamp:
                    continue
            
            filtered_events.append(event)
        
        # Replay events to build final state
        state = {
            "aggregate_id": aggregate_id,
            "class_id": class_id,
            "version": 0,
            "created_at": None,
            "updated_at": None,
            "deleted": False,
            "data": {}
        }
        
        for event in filtered_events:
            state = self._apply_event(state, event['content'])
        
        # Calculate state hash for verification
        state_hash = hashlib.sha256(
            json.dumps(state, sort_keys=True, default=str).encode()
        ).hexdigest()
        
        return {
            "aggregate_id": aggregate_id,
            "status": "REPLAYED",
            "event_count": len(filtered_events),
            "total_events": len(events),
            "final_state": state,
            "state_hash": state_hash,
            "replay_timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    def _apply_event(self, state: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply a single event to the current state
        
        This is the core of Event Sourcing - each event transforms the state
        """
        command_type = event.get('command_type')
        
        if command_type in ('CREATE_INSTANCE', 'BULK_CREATE_INSTANCES'):
            # Create event - initialize state
            state['version'] = 1
            state['created_at'] = event.get('created_at', event.get('timestamp'))
            state['updated_at'] = state['created_at']
            state['data'] = event.get('payload', {})
            state['deleted'] = False
            
        elif command_type == 'UPDATE_INSTANCE':
            # Update event - merge changes
            state['version'] += 1
            state['updated_at'] = event.get('timestamp')
            payload = event.get('payload', {})
            state['data'].update(payload)
            
        elif command_type == 'DELETE_INSTANCE':
            # Delete event - mark as deleted but preserve data
            state['version'] += 1
            state['deleted'] = True
            state['deleted_at'] = event.get('timestamp')
            state['deleted_by'] = event.get('created_by', 'unknown')
            
        # Add event metadata
        state['last_event_id'] = event.get('command_id')
        state['last_sequence'] = event.get('sequence_number', state.get('last_sequence', 0))
        
        return state
    
    async def replay_all_aggregates(
        self,
        db_name: str,
        class_id: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Replay all aggregates of a specific class
        
        Returns:
            Dictionary of all aggregate states
        """
        print(f"🔄 Replaying all {class_id} aggregates in {db_name}")

        aggregate_ids = {
            entry["aggregate_id"]
            for entry in await asyncio.to_thread(self._list_current_layout_index_entries, class_id=class_id)
            if entry.get("aggregate_id")
        }

        if not aggregate_ids:
            prefix = f"{db_name}/{class_id}/"
            objects = await self._list_all_objects_async(prefix=prefix)
            for obj in objects:
                parts = obj["Key"].split('/')
                if len(parts) >= 3:
                    aggregate_ids.add(parts[2])

        if not aggregate_ids:
            return {
                "status": "NO_AGGREGATES",
                "aggregate_count": 0,
                "aggregates": {}
            }
        
        # Replay each aggregate
        results = {}
        for aggregate_id in sorted(aggregate_ids)[:limit]:
            result = await self.replay_aggregate(db_name, class_id, aggregate_id)
            if result.get("status") == "REPLAYED":
                results[aggregate_id] = result
        
        return {
            "status": "REPLAYED",
            "aggregate_count": len(results),
            "total_aggregates": len(aggregate_ids),
            "aggregates": results,
            "replay_timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    async def point_in_time_replay(
        self,
        db_name: str,
        class_id: str,
        aggregate_id: str,
        target_time: datetime
    ) -> Dict[str, Any]:
        """
        Replay aggregate state at a specific point in time
        
        This enables time-travel queries - see the state as it was at any past moment
        """
        print(f"⏰ Point-in-time replay for {aggregate_id} at {target_time}")
        
        return await self.replay_aggregate(
            db_name=db_name,
            class_id=class_id,
            aggregate_id=aggregate_id,
            up_to_timestamp=target_time
        )
    
    async def verify_replay_determinism(
        self,
        db_name: str,
        class_id: str,
        aggregate_id: str
    ) -> Dict[str, Any]:
        """
        Verify that replaying produces deterministic results
        
        Replays multiple times and ensures identical results
        """
        print(f"🔍 Verifying replay determinism for {aggregate_id}")
        
        # Replay 3 times
        replays = []
        statuses = []
        for _i in range(3):
            result = await self.replay_aggregate(db_name, class_id, aggregate_id)
            statuses.append(str(result.get("status") or "UNKNOWN"))
            replays.append(str(result.get("state_hash") or f"status:{statuses[-1]}"))

        # Check all hashes are identical
        all_identical = len(set(replays)) == 1
        
        return {
            "aggregate_id": aggregate_id,
            "deterministic": all_identical,
            "hashes": replays,
            "verification": "PASSED" if all_identical else "FAILED"
        }
    
    async def get_aggregate_history(
        self,
        db_name: str,
        class_id: str,
        aggregate_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get complete event history for an aggregate
        
        Returns all events in chronological order
        """
        print(f"📜 Getting history for {aggregate_id}")

        current_layout_events, current_layout_scan_events, legacy_events = await asyncio.gather(
            asyncio.to_thread(
                self._load_current_layout_events,
                db_name=db_name,
                class_id=class_id,
                aggregate_id=aggregate_id,
            ),
            self._load_current_layout_events_from_objects(
                db_name=db_name,
                class_id=class_id,
                aggregate_id=aggregate_id,
            ),
            asyncio.to_thread(
                self._load_legacy_events,
                db_name=db_name,
                class_id=class_id,
                aggregate_id=aggregate_id,
            ),
        )
        events = self._merge_event_collections(current_layout_events, current_layout_scan_events, legacy_events)

        if not events:
            return []

        history = []
        for event in events:
            content = event["content"]
            history.append({
                "timestamp": event["timestamp"].isoformat() if hasattr(event["timestamp"], "isoformat") else str(event["timestamp"]),
                "key": event["key"],
                "size": None,
                "command_type": content.get('command_type'),
                "command_id": content.get('command_id'),
                "sequence_number": content.get('sequence_number', 0),
                "created_by": content.get('created_by', 'unknown')
            })

        history.sort(key=lambda x: (x['sequence_number'], x['timestamp']))

        return history


async def demo_replay():
    """
    Demo the replay functionality
    """
    replay_service = EventReplayService()
    
    # Test replay for a sample aggregate
    result = await replay_service.replay_aggregate(
        db_name="integration_test_db",
        class_id="IntegrationProduct",
        aggregate_id="IntegrationProduct_inst_08121140"
    )
    
    print("\n📊 REPLAY RESULT:")
    print(f"   Status: {result['status']}")
    print(f"   Events: {result['event_count']}")
    if result['final_state']:
        print(f"   Final Version: {result['final_state']['version']}")
        print(f"   State Hash: {result['state_hash'][:16]}...")
    
    # Verify determinism
    determinism = await replay_service.verify_replay_determinism(
        db_name="integration_test_db",
        class_id="IntegrationProduct",
        aggregate_id="IntegrationProduct_inst_08121140"
    )
    
    print("\n🔍 DETERMINISM CHECK:")
    print(f"   Result: {determinism['verification']}")
    print(f"   Hashes: {determinism['hashes'][0][:16]}...")
    
    return result


if __name__ == "__main__":
    result = asyncio.run(demo_replay())
    print("\n✅ Event Replay Service ready!")
