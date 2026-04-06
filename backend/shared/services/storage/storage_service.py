"""
Storage Service for S3/MinIO operations
S3 호환 객체 스토리지 연동 서비스
"""

import hashlib
import json
import logging
import asyncio
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Optional, AsyncIterator, Tuple, BinaryIO, Union

try:
    import boto3
    from botocore.client import BaseClient
    from botocore.exceptions import ClientError
    HAS_BOTO3 = True
except ImportError:
    # boto3 not available - S3 storage will be disabled
    boto3 = None
    BaseClient = None
    ClientError = Exception
    HAS_BOTO3 = False

from shared.models.commands import CommandType
from shared.errors.infra_errors import StorageUnavailableError
from shared.observability.tracing import trace_storage_operation
from shared.services.storage.s3_client_config import build_s3_client_config
from shared.services.storage.s3_error_utils import client_error_code as _client_error_code

if TYPE_CHECKING:
    from shared.config.settings import ApplicationSettings

logger = logging.getLogger(__name__)
class StorageService:
    """
    S3/MinIO 스토리지 서비스 - Event Sourcing 지원
    
    Event Sourcing 원칙에 따른 인스턴스 삭제 정책:
    
    🔥 중요: 삭제는 상태의 변화일 뿐, 정보의 소멸이 아닙니다.
    
    삭제 처리 원칙:
    1. 모든 Command 로그는 영구 보존됩니다 (감사 추적 목적)
    2. DELETE_INSTANCE Command도 다른 Command와 동일하게 S3에 저장됩니다
    3. 삭제된 인스턴스도 replay를 통해 완전한 상태 조회가 가능합니다
    4. 삭제 정보는 _metadata.deleted 플래그와 상세 정보로 기록됩니다
    
    API 레벨 처리:
    - replay_instance_state()는 삭제된 인스턴스도 전체 상태를 반환합니다
    - 상위 API에서 is_instance_deleted()로 삭제 여부를 확인해야 합니다
    - 삭제된 인스턴스에 대해서는 적절한 HTTP 상태 코드와 메시지를 반환해야 합니다
    
    복구 기능:
    - Event Sourcing의 장점으로 삭제된 인스턴스도 이론적으로 복구 가능합니다
    - 새로운 RESTORE_INSTANCE Command를 추가하여 복구 기능을 구현할 수 있습니다
    """
    
    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
        use_ssl: bool = False,
        ssl_verify: Optional[Union[bool, str]] = None,
    ):
        """
        스토리지 서비스 초기화
        
        Args:
            endpoint_url: S3/MinIO 엔드포인트 URL
            access_key: 액세스 키
            secret_key: 시크릿 키
            region: 리전 (기본값: us-east-1)
            use_ssl: SSL 사용 여부
        """
        if not HAS_BOTO3:
            raise ImportError("boto3 is required for S3 storage functionality. Install with: pip install boto3")
        
        self.endpoint_url = endpoint_url
        client_config = build_s3_client_config(endpoint_url, extra_path_style_hosts={"lakefs"})
        client_kwargs: Dict[str, Any] = {
            "endpoint_url": endpoint_url,
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "region_name": region,
            "use_ssl": use_ssl,
            "config": client_config,
        }
        if use_ssl and ssl_verify is not None:
            client_kwargs["verify"] = ssl_verify

        self.client = boto3.client("s3", **client_kwargs)

    def _raise_storage_unavailable(
        self,
        *,
        exc: Exception,
        operation: str,
        bucket: str,
        path: Optional[str] = None,
        message: Optional[str] = None,
    ) -> None:
        detail = message or f"Storage operation failed during {operation}"
        raise StorageUnavailableError(
            detail,
            operation=operation,
            bucket=bucket,
            path=path,
            cause=exc,
        ) from exc

    @staticmethod
    def _read_body_and_close(body: Any) -> bytes:
        try:
            return body.read()
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                close()
        
    @trace_storage_operation("s3.create_bucket")
    async def create_bucket(self, bucket_name: str) -> bool:
        """
        버킷 생성
        
        Args:
            bucket_name: 버킷 이름
            
        Returns:
            성공 여부
        """
        def _create() -> bool:
            try:
                self.client.create_bucket(Bucket=bucket_name)
                return True
            except ClientError as e:
                if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                    return True
                raise

        return await asyncio.to_thread(_create)
            
    @trace_storage_operation("s3.bucket_exists")
    async def bucket_exists(self, bucket_name: str) -> bool:
        """
        버킷 존재 여부 확인
        
        Args:
            bucket_name: 버킷 이름
            
        Returns:
            존재 여부
        """
        def _exists() -> bool:
            try:
                self.client.head_bucket(Bucket=bucket_name)
                return True
            except ClientError as exc:
                code = str((exc.response.get("Error") or {}).get("Code") or "")
                if code in {"404", "NoSuchBucket", "NotFound"}:
                    return False
                raise StorageUnavailableError(
                    f"Failed to probe bucket {bucket_name}: {code or 'ClientError'}"
                ) from exc

        return await asyncio.to_thread(_exists)
            
    @trace_storage_operation("s3.save_json")
    async def save_json(
        self,
        bucket: str,
        key: str,
        data: Dict[str, Any],
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """
        JSON 데이터를 S3에 저장하고 체크섬 반환
        
        Args:
            bucket: 버킷 이름
            key: 객체 키 (경로)
            data: 저장할 JSON 데이터
            metadata: 추가 메타데이터
            
        Returns:
            SHA256 체크섬
        """
        # JSON 직렬화
        json_data = json.dumps(data, ensure_ascii=False, indent=2, default=str)
        json_bytes = json_data.encode('utf-8')
        
        # SHA256 체크섬 계산
        checksum = hashlib.sha256(json_bytes).hexdigest()
        
        # 메타데이터 준비
        object_metadata = metadata or {}
        object_metadata.update({
            'checksum': checksum,
            'content-type': 'application/json',
            'created-at': datetime.now(timezone.utc).isoformat()
        })
        
        def _put() -> None:
            self.client.put_object(
                Bucket=bucket,
                Key=key,
                Body=json_bytes,
                ContentType='application/json',
                Metadata=object_metadata,
            )

        await asyncio.to_thread(_put)
        
        return checksum

    @trace_storage_operation("s3.save_bytes")
    async def save_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str = 'application/octet-stream',
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Raw bytes를 S3에 저장하고 체크섬 반환

        Args:
            bucket: 버킷 이름
            key: 객체 키 (경로)
            data: 저장할 바이트 데이터
            content_type: MIME 타입
            metadata: 추가 메타데이터

        Returns:
            SHA256 체크섬
        """
        if not isinstance(data, (bytes, bytearray)):
            raise ValueError("data must be bytes")

        blob = bytes(data)
        checksum = hashlib.sha256(blob).hexdigest()

        object_metadata = metadata or {}
        object_metadata.update({
            'checksum': checksum,
            'content-type': content_type,
            'created-at': datetime.now(timezone.utc).isoformat(),
        })

        def _put() -> None:
            self.client.put_object(
                Bucket=bucket,
                Key=key,
                Body=blob,
                ContentType=content_type,
                Metadata=object_metadata,
            )

        await asyncio.to_thread(_put)
        
        return checksum

    @trace_storage_operation("s3.save_fileobj")
    async def save_fileobj(
        self,
        bucket: str,
        key: str,
        fileobj: BinaryIO,
        content_type: str = 'application/octet-stream',
        metadata: Optional[Dict[str, str]] = None,
        checksum: Optional[str] = None,
    ) -> str:
        """
        Stream a file-like object into S3 and return a checksum.

        The file object must be seekable so we can compute the checksum and
        reset the cursor for upload.
        """
        if not hasattr(fileobj, "read"):
            raise ValueError("fileobj must be file-like")
        if not hasattr(fileobj, "seek"):
            raise ValueError("fileobj must be seekable")

        def _compute_checksum() -> str:
            fileobj.seek(0)
            hasher = hashlib.sha256()
            while True:
                chunk = fileobj.read(1024 * 1024)
                if not chunk:
                    break
                hasher.update(chunk)
            return hasher.hexdigest()

        if not checksum:
            checksum = await asyncio.to_thread(_compute_checksum)

        object_metadata = metadata or {}
        object_metadata.update({
            'checksum': checksum,
            'content-type': content_type,
            'created-at': datetime.now(timezone.utc).isoformat(),
        })

        def _put() -> None:
            fileobj.seek(0)
            self.client.upload_fileobj(
                fileobj,
                Bucket=bucket,
                Key=key,
                ExtraArgs={
                    "ContentType": content_type,
                    "Metadata": object_metadata,
                },
            )

        await asyncio.to_thread(_put)

        return checksum
        
    @trace_storage_operation("s3.load_json")
    async def load_json(self, bucket: str, key: str) -> Dict[str, Any]:
        """
        S3에서 JSON 데이터 로드
        
        Args:
            bucket: 버킷 이름
            key: 객체 키 (경로)
            
        Returns:
            JSON 데이터
        """
        try:
            response = await asyncio.to_thread(self.client.get_object, Bucket=bucket, Key=key)
            json_bytes = await asyncio.to_thread(self._read_body_and_close, response["Body"])
            json_data = json_bytes.decode('utf-8')
            return json.loads(json_data)
        except ClientError as e:
            if _client_error_code(e) == 'NoSuchKey':
                raise FileNotFoundError(f"Object not found: {bucket}/{key}")
            self._raise_storage_unavailable(
                exc=e,
                operation="load_json",
                bucket=bucket,
                path=key,
            )

    @trace_storage_operation("s3.load_bytes")
    async def load_bytes(self, bucket: str, key: str) -> bytes:
        """
        S3에서 Raw bytes 로드

        Args:
            bucket: 버킷 이름
            key: 객체 키 (경로)

        Returns:
            bytes
        """
        try:
            response = await asyncio.to_thread(self.client.get_object, Bucket=bucket, Key=key)
            return await asyncio.to_thread(self._read_body_and_close, response["Body"])
        except ClientError as e:
            if _client_error_code(e) == 'NoSuchKey':
                raise FileNotFoundError(f"Object not found: {bucket}/{key}")
            self._raise_storage_unavailable(
                exc=e,
                operation="load_bytes",
                bucket=bucket,
                path=key,
            )

    @trace_storage_operation("s3.load_bytes_lines")
    async def load_bytes_lines(
        self,
        bucket: str,
        key: str,
        *,
        max_lines: int,
        max_bytes: Optional[int] = None,
    ) -> bytes:
        """
        Load up to `max_lines` newline-delimited lines from the start of an object.

        This is intended for "head sampling" large text objects (e.g., CSV) without
        reading the full object into memory.
        """
        resolved_lines = max(0, int(max_lines))
        if resolved_lines <= 0:
            return b""
        resolved_bytes = int(max_bytes) if max_bytes is not None else None
        if resolved_bytes is not None and resolved_bytes <= 0:
            resolved_bytes = None

        def _load() -> bytes:
            response = self.client.get_object(Bucket=bucket, Key=key)
            body = response.get("Body")
            if body is None:
                return b""
            out = bytearray()
            count = 0
            try:
                # StreamingBody.iter_lines() yields lines without the line terminator.
                for line in body.iter_lines():
                    out.extend(line or b"")
                    out.extend(b"\n")
                    count += 1
                    if count >= resolved_lines:
                        break
                    if resolved_bytes is not None and len(out) >= resolved_bytes:
                        break
            finally:
                try:
                    body.close()
                except Exception as close_exc:
                    logger.warning(
                        "Failed to close storage stream for %s/%s: %s",
                        bucket,
                        key,
                        close_exc,
                        exc_info=True,
                    )
            return bytes(out)

        try:
            return await asyncio.to_thread(_load)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(f"Object not found: {bucket}/{key}")
            raise
            
    @trace_storage_operation("s3.verify_checksum")
    async def verify_checksum(
        self,
        bucket: str,
        key: str,
        expected_checksum: str
    ) -> bool:
        """
        저장된 파일의 체크섬 검증
        
        Args:
            bucket: 버킷 이름
            key: 객체 키 (경로)
            expected_checksum: 예상 체크섬
            
        Returns:
            검증 성공 여부
        """
        try:
            response = await asyncio.to_thread(self.client.get_object, Bucket=bucket, Key=key)
            content = await asyncio.to_thread(self._read_body_and_close, response["Body"])
            actual_checksum = hashlib.sha256(content).hexdigest()
            return actual_checksum == expected_checksum
        except ClientError as e:
            if _client_error_code(e) == "NoSuchKey":
                raise FileNotFoundError(f"Object not found: {bucket}/{key}")
            self._raise_storage_unavailable(
                exc=e,
                operation="verify_checksum",
                bucket=bucket,
                path=key,
            )
            
    @trace_storage_operation("s3.delete_object")
    async def delete_object(self, bucket: str, key: str) -> bool:
        """
        S3 객체 삭제
        
        Args:
            bucket: 버킷 이름
            key: 객체 키 (경로)
            
        Returns:
            성공 여부
        """
        try:
            self.client.delete_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            code = str((exc.response.get("Error") or {}).get("Code") or "")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise StorageUnavailableError(
                f"Failed to delete object s3://{bucket}/{key}: {code or 'ClientError'}"
            ) from exc

    @trace_storage_operation("s3.delete_prefix")
    async def delete_prefix(
        self,
        bucket: str,
        prefix: str,
    ) -> int:
        """
        Delete all objects under a prefix.

        This is used to implement stable output paths where repeated builds overwrite
        the same logical dataset path without leaving stale part files behind.
        """
        prefix = (prefix or "").lstrip("/")
        if not prefix:
            raise ValueError("prefix is required")
        if not prefix.endswith("/"):
            prefix = f"{prefix}/"

        deleted = 0
        continuation_token: Optional[str] = None
        while True:
            kwargs: Dict[str, Any] = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token

            try:
                response = await asyncio.to_thread(self.client.list_objects_v2, **kwargs)
            except ClientError as exc:
                self._raise_storage_unavailable(
                    exc=exc,
                    operation="delete_prefix:list",
                    bucket=bucket,
                    path=prefix,
                )
            contents = response.get("Contents", []) or []
            keys = [str(obj.get("Key") or "") for obj in contents if str(obj.get("Key") or "").startswith(prefix)]

            if keys:
                try:
                    delete_response = await asyncio.to_thread(
                        self.client.delete_objects,
                        Bucket=bucket,
                        Delete={"Objects": [{"Key": key} for key in keys], "Quiet": True},
                    )
                except ClientError as exc:
                    self._raise_storage_unavailable(
                        exc=exc,
                        operation="delete_prefix:delete",
                        bucket=bucket,
                        path=prefix,
                    )
                errors = delete_response.get("Errors", []) if isinstance(delete_response, dict) else []
                if errors:
                    first_error = errors[0] if isinstance(errors[0], dict) else {}
                    reason = str(first_error.get("Message") or first_error.get("Code") or "delete failed")
                    self._raise_storage_unavailable(
                        exc=RuntimeError(reason),
                        operation="delete_prefix:delete",
                        bucket=bucket,
                        path=str(first_error.get("Key") or prefix),
                        message=f"Failed to delete one or more objects under {bucket}/{prefix}",
                    )
                deleted += len(keys)

            if not response.get("IsTruncated"):
                break
            continuation_token = response.get("NextContinuationToken")

        return deleted
            
    @trace_storage_operation("s3.list_objects")
    async def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        max_keys: int = 1000
    ) -> list:
        """
        버킷의 객체 목록 조회
        
        Args:
            bucket: 버킷 이름
            prefix: 경로 프리픽스
            max_keys: 최대 결과 수
            
        Returns:
            객체 목록
        """
        try:
            response = await asyncio.to_thread(
                self.client.list_objects_v2,
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=max_keys,
            )
            return response.get('Contents', [])
        except ClientError as exc:
            self._raise_storage_unavailable(
                exc=exc,
                operation="list_objects",
                bucket=bucket,
                path=prefix,
            )

    @trace_storage_operation("s3.list_objects_paginated")
    async def list_objects_paginated(
        self,
        bucket: str,
        prefix: str = "",
        max_keys: int = 1000,
        continuation_token: Optional[str] = None,
    ) -> Tuple[list, Optional[str]]:
        """
        Paginated object listing (returns next continuation token if more).
        """
        try:
            params: Dict[str, Any] = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": max_keys}
            if continuation_token:
                params["ContinuationToken"] = continuation_token
            response = await asyncio.to_thread(self.client.list_objects_v2, **params)
            contents = response.get("Contents", []) or []
            token = response.get("NextContinuationToken") if response.get("IsTruncated") else None
            return contents, token
        except ClientError as exc:
            self._raise_storage_unavailable(
                exc=exc,
                operation="list_objects_paginated",
                bucket=bucket,
                path=prefix,
            )

    @trace_storage_operation("s3.iter_objects")
    async def iter_objects(
        self,
        bucket: str,
        prefix: str = "",
        max_keys: int = 1000,
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Async iterator over all objects under prefix (pagination-aware).
        """
        token: Optional[str] = None
        while True:
            contents, token = await self.list_objects_paginated(
                bucket=bucket,
                prefix=prefix,
                max_keys=max_keys,
                continuation_token=token,
            )
            for obj in contents:
                if isinstance(obj, dict):
                    yield obj
            if not token:
                break
            
    @trace_storage_operation("s3.get_object_metadata")
    async def get_object_metadata(
        self,
        bucket: str,
        key: str
    ) -> Dict[str, Any]:
        """
        객체 메타데이터 조회
        
        Args:
            bucket: 버킷 이름
            key: 객체 키 (경로)
            
        Returns:
            메타데이터
        """
        try:
            response = await asyncio.to_thread(self.client.head_object, Bucket=bucket, Key=key)
            return {
                'size': response.get('ContentLength'),
                'last_modified': response.get('LastModified'),
                'etag': response.get('ETag'),
                'metadata': response.get('Metadata', {})
            }
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise FileNotFoundError(f"Object not found: {bucket}/{key}")
            raise
            
    def generate_instance_path(
        self,
        db_name: str,
        class_id: str,
        instance_id: str,
        command_id: str
    ) -> str:
        """
        인스턴스 이벤트 저장 경로 생성
        
        Args:
            db_name: 데이터베이스 이름
            class_id: 클래스 ID
            instance_id: 인스턴스 ID
            command_id: 명령 ID
            
        Returns:
            S3 경로
        """
        return f"{db_name}/{class_id}/{instance_id}/{command_id}.json"
        
    @trace_storage_operation("s3.get_all_commands_for_instance")
    async def get_all_commands_for_instance(
        self,
        bucket: str,
        db_name: str,
        class_id: str,
        instance_id: str
    ) -> list:
        """
        특정 인스턴스의 모든 Command 파일 목록 조회
        
        Args:
            bucket: 버킷 이름
            db_name: 데이터베이스 이름
            class_id: 클래스 ID
            instance_id: 인스턴스 ID
            
        Returns:
            Command 파일 경로 목록 (시간순 정렬)
        """
        prefix = f"{db_name}/{class_id}/{instance_id}/"
        
        try:
            # 모든 파일 목록 조회
            all_objects = []
            continuation_token = None
            
            while True:
                if continuation_token:
                    response = self.client.list_objects_v2(
                        Bucket=bucket,
                        Prefix=prefix,
                        ContinuationToken=continuation_token
                    )
                else:
                    response = self.client.list_objects_v2(
                        Bucket=bucket,
                        Prefix=prefix
                    )
                
                if 'Contents' in response:
                    all_objects.extend(response['Contents'])
                
                # 페이지네이션 처리
                if response.get('IsTruncated'):
                    continuation_token = response.get('NextContinuationToken')
                else:
                    break
            
            # .json 파일만 필터링하고 시간순 정렬
            # Event Sourcing 원칙: 모든 Command는 감사 추적을 위해 보존되어야 함
            command_files = [
                obj for obj in all_objects 
                if obj['Key'].endswith('.json')
            ]
            command_files.sort(key=lambda x: x['LastModified'])
            
            return [obj['Key'] for obj in command_files]
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                raise FileNotFoundError(f"Bucket not found: {bucket}")
            raise

    @trace_storage_operation("s3.list_command_files")
    async def list_command_files(
        self,
        *,
        bucket: str,
        prefix: str,
    ) -> list[str]:
        """
        List command JSON objects under a prefix (pagination-aware, sorted by LastModified).

        The Action writeback worker uses a branch-aware prefix:
        `{db_name}/{branch}/{class_id}/{instance_id}/`.
        """
        prefix = (prefix or "").lstrip("/")
        if not prefix:
            raise ValueError("prefix is required")
        if not prefix.endswith("/"):
            prefix = f"{prefix}/"

        objects: list[Dict[str, Any]] = []
        token: Optional[str] = None
        while True:
            contents, token = await self.list_objects_paginated(
                bucket=bucket,
                prefix=prefix,
                max_keys=1000,
                continuation_token=token,
            )
            for obj in contents:
                if not isinstance(obj, dict):
                    continue
                key = str(obj.get("Key") or "")
                if not key.startswith(prefix) or not key.endswith(".json"):
                    continue
                objects.append(obj)
            if not token:
                break

        objects.sort(key=lambda obj: obj.get("LastModified") or datetime.fromtimestamp(0, tz=timezone.utc))
        return [str(obj.get("Key") or "") for obj in objects if str(obj.get("Key") or "").strip()]
            
    @trace_storage_operation("s3.replay_instance_state")
    async def replay_instance_state(
        self,
        bucket: str,
        command_files: list,
        *,
        strict: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Command 파일들을 순차적으로 읽어 인스턴스의 최종 상태 재구성
        
        Event Sourcing 원칙에 따라 삭제된 인스턴스도 완전한 상태 정보를 반환합니다.
        삭제는 상태의 변화일 뿐 정보의 소멸이 아닙니다.
        
        Args:
            bucket: 버킷 이름
            command_files: Command 파일 경로 목록 (시간순)
            
        Returns:
            재구성된 인스턴스의 최종 상태 (삭제된 인스턴스도 포함)
            Command 파일이 없거나 처리 불가능한 경우에만 None 반환
        """
        instance_state = None
        command_history = []
        
        for file_key in command_files:
            try:
                # Command 파일 읽기
                command_data = await self.load_json(bucket, file_key)

                # Normalize shape:
                # - Compatibility shape: {command_type, payload, ...}
                # - Current instance-worker snapshot shape: {command: {...}, payload: <full/merged>, ...}
                command_record = command_data
                if isinstance(command_data, dict) and isinstance(command_data.get("command"), dict):
                    command_record = command_data["command"]

                command_type = (
                    (command_record.get("command_type") if isinstance(command_record, dict) else None)
                    or (command_data.get("command_type") if isinstance(command_data, dict) else None)
                )

                command_id = (
                    (command_record.get("command_id") if isinstance(command_record, dict) else None)
                    or (command_data.get("command_id") if isinstance(command_data, dict) else None)
                )
                instance_id = (
                    (command_record.get("instance_id") if isinstance(command_record, dict) else None)
                    or (command_data.get("instance_id") if isinstance(command_data, dict) else None)
                )
                class_id = (
                    (command_record.get("class_id") if isinstance(command_record, dict) else None)
                    or (command_data.get("class_id") if isinstance(command_data, dict) else None)
                )
                db_name = (
                    (command_record.get("db_name") if isinstance(command_record, dict) else None)
                    or (command_data.get("db_name") if isinstance(command_data, dict) else None)
                )
                created_by = (
                    (command_record.get("created_by") if isinstance(command_record, dict) else None)
                    or (command_data.get("created_by") if isinstance(command_data, dict) else None)
                )
                timestamp = (
                    (command_record.get("created_at") if isinstance(command_record, dict) else None)
                    or (command_data.get("created_at") if isinstance(command_data, dict) else None)
                    or (command_data.get("updated_at") if isinstance(command_data, dict) else None)
                    or (command_data.get("deleted_at") if isinstance(command_data, dict) else None)
                )

                payload = None
                if isinstance(command_data, dict):
                    payload = command_data.get("payload")
                if payload is None and isinstance(command_record, dict):
                    payload = command_record.get("payload")
                if payload is None:
                    payload = {}
                
                # Command 이력 추가
                command_history.append({
                    'command_id': command_id,
                    'command_type': command_type,
                    'timestamp': timestamp,
                    'file': file_key
                })
                
                # Command 유형에 따라 상태 업데이트
                if command_type in (
                    CommandType.CREATE_INSTANCE.value,
                    CommandType.BULK_CREATE_INSTANCES.value,
                ):
                    # 인스턴스 생성
                    instance_state = {
                        'instance_id': instance_id,
                        'class_id': class_id,
                        'db_name': db_name,
                        **(payload if isinstance(payload, dict) else {}),
                        '_metadata': {
                            'created_at': timestamp,
                            'created_by': created_by,
                            'version': 1,
                            'command_history': command_history
                        }
                    }
                    
                elif command_type == CommandType.UPDATE_INSTANCE.value and instance_state:
                    # 인스턴스 업데이트
                    updates = payload if isinstance(payload, dict) else {}
                    # 메타데이터는 보존하면서 데이터 업데이트
                    metadata = instance_state.get('_metadata', {})
                    instance_state.update(updates)
                    instance_state['_metadata'] = metadata
                    instance_state['_metadata']['updated_at'] = timestamp
                    instance_state['_metadata']['updated_by'] = created_by
                    instance_state['_metadata']['version'] = metadata.get('version', 1) + 1
                    
                elif command_type == CommandType.DELETE_INSTANCE.value:
                    # 인스턴스 삭제 상태 기록
                    # Event Sourcing 원칙: 삭제도 하나의 상태 변화로 기록하여 감사 추적 가능
                    if instance_state:
                        instance_state['_metadata']['deleted'] = True
                        instance_state['_metadata']['deleted_at'] = timestamp
                        instance_state['_metadata']['deleted_by'] = created_by
                        instance_state['_metadata']['deletion_command_id'] = command_id
                        instance_state['_metadata']['deletion_reason'] = (
                            payload.get('reason', 'No reason provided')
                            if isinstance(payload, dict)
                            else 'No reason provided'
                        )
                    else:
                        # 생성 없이 삭제 Command만 있는 경우 (데이터 정합성 문제)
                        # 최소한의 삭제 정보라도 기록
                        instance_state = {
                            'instance_id': instance_id,
                            'class_id': class_id,
                            'db_name': db_name,
                            '_metadata': {
                                'deleted': True,
                                'deleted_at': timestamp,
                                'deleted_by': created_by,
                                'deletion_command_id': command_id,
                                'deletion_reason': (
                                    payload.get('reason', 'No reason provided')
                                    if isinstance(payload, dict)
                                    else 'No reason provided'
                                ),
                                'orphan_deletion': True,  # 생성 Command 없이 삭제된 경우
                                'version': 1
                            }
                        }
                
            except Exception as exc:
                if strict:
                    raise RuntimeError(f"Failed to process command file {file_key}") from exc
                logger.error("Failed to process command file %s: %s", file_key, exc, exc_info=True)
                continue
        
        # 최종 상태에 Command 이력 포함
        if instance_state:
            instance_state['_metadata']['command_history'] = command_history
            instance_state['_metadata']['total_commands'] = len(command_history)
        
        return instance_state
    
    def is_instance_deleted(self, instance_state: Dict[str, Any]) -> bool:
        """
        인스턴스가 삭제된 상태인지 확인
        
        Args:
            instance_state: replay_instance_state로 재구성된 인스턴스 상태
            
        Returns:
            삭제 여부
        """
        if not instance_state:
            return False
        return instance_state.get('_metadata', {}).get('deleted', False)
    
    def get_deletion_info(self, instance_state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        삭제된 인스턴스의 삭제 정보 반환
        
        Args:
            instance_state: replay_instance_state로 재구성된 인스턴스 상태
            
        Returns:
            삭제 정보 (삭제된 경우) 또는 None
        """
        if not self.is_instance_deleted(instance_state):
            return None
            
        metadata = instance_state.get('_metadata', {})
        return {
            'deleted_at': metadata.get('deleted_at'),
            'deleted_by': metadata.get('deleted_by'),
            'deletion_command_id': metadata.get('deletion_command_id'),
            'deletion_reason': metadata.get('deletion_reason'),
            'is_orphan_deletion': metadata.get('orphan_deletion', False)
        }
        


def create_storage_service(settings: 'ApplicationSettings') -> Optional[StorageService]:
    """
    스토리지 서비스 팩토리 함수 (Anti-pattern 13 해결)
    
    Args:
        settings: 중앙화된 애플리케이션 설정 객체
        
    Returns:
        StorageService 인스턴스 또는 None (boto3 없는 경우)
        
    Note:
        이 함수는 더 이상 내부에서 환경변수를 로드하지 않습니다.
        모든 설정은 ApplicationSettings를 통해 중앙화되어 관리됩니다.
    """
    if not HAS_BOTO3:
        # S3 storage is optional for local development
        return None
        
    verify: Optional[Union[bool, str]] = None
    ca_bundle = (getattr(settings.storage, "minio_ssl_ca_bundle", None) or "").strip()
    if ca_bundle:
        verify = ca_bundle
    elif getattr(settings.storage, "minio_ssl_verify", None) is not None:
        verify = settings.storage.minio_ssl_verify

    return StorageService(
        endpoint_url=settings.storage.minio_endpoint_url,
        access_key=settings.storage.minio_access_key,
        secret_key=settings.storage.minio_secret_key,
        use_ssl=settings.storage.use_ssl,
        ssl_verify=verify,
    )
