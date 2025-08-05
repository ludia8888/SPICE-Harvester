"""
Search Configuration
Elasticsearch 인덱스 이름 규칙 중앙 관리
"""

import re
from typing import Optional


def sanitize_index_name(name: str) -> str:
    """
    Elasticsearch 인덱스 이름 규칙에 맞게 문자열을 정제합니다.
    
    Elasticsearch 인덱스 이름 규칙:
    - 소문자만 허용
    - 특수문자 중 dash(-), underscore(_), plus(+) 허용
    - 마침표(.)로 시작하면 안됨
    - 255바이트 이하
    - 공백 불허
    
    Args:
        name: 정제할 이름
        
    Returns:
        정제된 인덱스 이름
    """
    # 소문자 변환
    name = name.lower()
    
    # 공백을 underscore로 변환
    name = name.replace(' ', '_')
    
    # 허용되지 않는 특수문자를 underscore로 변환
    # 허용: a-z, 0-9, -, _, +
    name = re.sub(r'[^a-z0-9\-_+]', '_', name)
    
    # 연속된 underscore 제거
    name = re.sub(r'_+', '_', name)
    
    # 마침표로 시작하면 제거
    name = name.lstrip('.')
    
    # 시작과 끝의 underscore 제거
    name = name.strip('_')
    
    # 빈 문자열이면 기본값
    if not name:
        name = 'default'
    
    # 255바이트 제한
    if len(name.encode('utf-8')) > 255:
        # UTF-8 안전하게 자르기
        name = name.encode('utf-8')[:252].decode('utf-8', 'ignore')
    
    return name


def get_instances_index_name(db_name: str, version: Optional[str] = None) -> str:
    """
    인스턴스 데이터를 위한 Elasticsearch 인덱스 이름을 생성합니다.
    
    Args:
        db_name: 데이터베이스 이름
        version: 인덱스 버전 (Blue-Green 배포 등을 위한 선택적 버전)
        
    Returns:
        인스턴스 인덱스 이름
        
    Examples:
        >>> get_instances_index_name("MyDB")
        'mydb_instances'
        >>> get_instances_index_name("My-DB", "v2")
        'my_db_instances_v2'
    """
    base_name = sanitize_index_name(db_name)
    index_name = f"{base_name}_instances"
    
    if version:
        version = sanitize_index_name(version)
        index_name = f"{index_name}_{version}"
    
    return index_name


def get_ontologies_index_name(db_name: str, version: Optional[str] = None) -> str:
    """
    온톨로지 데이터를 위한 Elasticsearch 인덱스 이름을 생성합니다.
    
    Args:
        db_name: 데이터베이스 이름
        version: 인덱스 버전 (Blue-Green 배포 등을 위한 선택적 버전)
        
    Returns:
        온톨로지 인덱스 이름
        
    Examples:
        >>> get_ontologies_index_name("MyDB")
        'mydb_ontologies'
        >>> get_ontologies_index_name("My-DB", "v2")
        'my_db_ontologies_v2'
    """
    base_name = sanitize_index_name(db_name)
    index_name = f"{base_name}_ontologies"
    
    if version:
        version = sanitize_index_name(version)
        index_name = f"{index_name}_{version}"
    
    return index_name


def get_index_alias_name(index_name: str) -> str:
    """
    인덱스의 별칭(alias) 이름을 생성합니다.
    Blue-Green 배포 시 버전 관리를 위해 사용됩니다.
    
    Args:
        index_name: 실제 인덱스 이름
        
    Returns:
        인덱스 별칭
        
    Examples:
        >>> get_index_alias_name("mydb_instances_v2")
        'mydb_instances'
    """
    # 버전 접미사 제거 (_v숫자 패턴)
    alias = re.sub(r'_v\d+$', '', index_name)
    return alias


# 인덱스 설정 상수
DEFAULT_NUMBER_OF_SHARDS = 1
DEFAULT_NUMBER_OF_REPLICAS = 1
DEFAULT_REFRESH_INTERVAL = "1s"

# 인덱스 설정 템플릿
DEFAULT_INDEX_SETTINGS = {
    "number_of_shards": DEFAULT_NUMBER_OF_SHARDS,
    "number_of_replicas": DEFAULT_NUMBER_OF_REPLICAS,
    "refresh_interval": DEFAULT_REFRESH_INTERVAL,
    "analysis": {
        "analyzer": {
            "korean_analyzer": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": ["lowercase", "stop"]
            }
        }
    }
}