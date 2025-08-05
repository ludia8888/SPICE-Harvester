"""
Storage Service for S3/MinIO operations
S3 호환 객체 스토리지 연동 서비스
"""

import hashlib
import json
from datetime import datetime
from typing import Any, Dict, Optional

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from shared.config.service_config import ServiceConfig
from shared.models.commands import CommandType


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
        use_ssl: bool = False
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
        self.endpoint_url = endpoint_url
        self.client: BaseClient = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            use_ssl=use_ssl,
            verify=False  # MinIO 로컬 개발 시 SSL 검증 비활성화
        )
        
    async def create_bucket(self, bucket_name: str) -> bool:
        """
        버킷 생성
        
        Args:
            bucket_name: 버킷 이름
            
        Returns:
            성공 여부
        """
        try:
            self.client.create_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                return True
            raise
            
    async def bucket_exists(self, bucket_name: str) -> bool:
        """
        버킷 존재 여부 확인
        
        Args:
            bucket_name: 버킷 이름
            
        Returns:
            존재 여부
        """
        try:
            self.client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError:
            return False
            
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
            'created-at': datetime.utcnow().isoformat()
        })
        
        # S3에 업로드
        self.client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json_bytes,
            ContentType='application/json',
            Metadata=object_metadata
        )
        
        return checksum
        
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
            response = self.client.get_object(Bucket=bucket, Key=key)
            json_data = response['Body'].read().decode('utf-8')
            return json.loads(json_data)
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise FileNotFoundError(f"Object not found: {bucket}/{key}")
            raise
            
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
            response = self.client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read()
            actual_checksum = hashlib.sha256(content).hexdigest()
            return actual_checksum == expected_checksum
        except ClientError:
            return False
            
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
        except ClientError:
            return False
            
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
            response = self.client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            return response.get('Contents', [])
        except ClientError:
            return []
            
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
            response = self.client.head_object(Bucket=bucket, Key=key)
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
            
    async def replay_instance_state(
        self,
        bucket: str,
        command_files: list
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
                command_type = command_data.get('command_type')
                
                # Command 이력 추가
                command_history.append({
                    'command_id': command_data.get('command_id'),
                    'command_type': command_type,
                    'timestamp': command_data.get('created_at'),
                    'file': file_key
                })
                
                # Command 유형에 따라 상태 업데이트
                if command_type == CommandType.CREATE_INSTANCE.value:
                    # 인스턴스 생성
                    instance_state = {
                        'instance_id': command_data.get('instance_id'),
                        'class_id': command_data.get('class_id'),
                        'db_name': command_data.get('db_name'),
                        **command_data.get('payload', {}),
                        '_metadata': {
                            'created_at': command_data.get('created_at'),
                            'created_by': command_data.get('created_by'),
                            'version': 1,
                            'command_history': command_history
                        }
                    }
                    
                elif command_type == CommandType.UPDATE_INSTANCE.value and instance_state:
                    # 인스턴스 업데이트
                    updates = command_data.get('payload', {})
                    # 메타데이터는 보존하면서 데이터 업데이트
                    metadata = instance_state.get('_metadata', {})
                    instance_state.update(updates)
                    instance_state['_metadata'] = metadata
                    instance_state['_metadata']['updated_at'] = command_data.get('created_at')
                    instance_state['_metadata']['updated_by'] = command_data.get('created_by')
                    instance_state['_metadata']['version'] = metadata.get('version', 1) + 1
                    
                elif command_type == CommandType.DELETE_INSTANCE.value:
                    # 인스턴스 삭제 상태 기록
                    # Event Sourcing 원칙: 삭제도 하나의 상태 변화로 기록하여 감사 추적 가능
                    if instance_state:
                        instance_state['_metadata']['deleted'] = True
                        instance_state['_metadata']['deleted_at'] = command_data.get('created_at')
                        instance_state['_metadata']['deleted_by'] = command_data.get('created_by')
                        instance_state['_metadata']['deletion_command_id'] = command_data.get('command_id')
                        instance_state['_metadata']['deletion_reason'] = command_data.get('payload', {}).get('reason', 'No reason provided')
                    else:
                        # 생성 없이 삭제 Command만 있는 경우 (데이터 정합성 문제)
                        # 최소한의 삭제 정보라도 기록
                        instance_state = {
                            'instance_id': command_data.get('instance_id'),
                            'class_id': command_data.get('class_id'),
                            'db_name': command_data.get('db_name'),
                            '_metadata': {
                                'deleted': True,
                                'deleted_at': command_data.get('created_at'),
                                'deleted_by': command_data.get('created_by'),
                                'deletion_command_id': command_data.get('command_id'),
                                'deletion_reason': command_data.get('payload', {}).get('reason', 'No reason provided'),
                                'orphan_deletion': True,  # 생성 Command 없이 삭제된 경우
                                'version': 1
                            }
                        }
                
            except Exception as e:
                # 개별 Command 처리 실패 시 로그만 남기고 계속 진행
                import logging
                logging.error(f"Failed to process command file {file_key}: {e}")
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
        


def create_storage_service(
    endpoint_url: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None
) -> StorageService:
    """
    스토리지 서비스 팩토리 함수
    
    Args:
        endpoint_url: S3/MinIO 엔드포인트 URL (환경변수 우선)
        access_key: 액세스 키 (환경변수 우선)
        secret_key: 시크릿 키 (환경변수 우선)
        
    Returns:
        StorageService 인스턴스
    """
    config = ServiceConfig()
    
    # 환경변수에서 값 가져오기 (인자보다 우선)
    import os
    endpoint = os.getenv('MINIO_ENDPOINT_URL', endpoint_url or 'http://localhost:9000')
    access = os.getenv('MINIO_ACCESS_KEY', access_key or 'minioadmin')
    secret = os.getenv('MINIO_SECRET_KEY', secret_key or 'minioadmin123')
    
    return StorageService(
        endpoint_url=endpoint,
        access_key=access,
        secret_key=secret,
        use_ssl=endpoint.startswith('https')
    )