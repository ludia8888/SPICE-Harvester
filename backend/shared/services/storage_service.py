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

from backend.shared.config.service_config import ServiceConfig


class StorageService:
    """S3/MinIO 스토리지 서비스"""
    
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
        
    def generate_latest_path(
        self,
        db_name: str,
        class_id: str,
        instance_id: str
    ) -> str:
        """
        인스턴스 최신 상태 저장 경로 생성
        
        Args:
            db_name: 데이터베이스 이름
            class_id: 클래스 ID
            instance_id: 인스턴스 ID
            
        Returns:
            S3 경로
        """
        return f"{db_name}/{class_id}/{instance_id}/latest.json"


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