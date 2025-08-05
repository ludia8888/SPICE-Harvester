"""
Instance Service for TerminusDB
인스턴스 데이터 조회 최적화 서비스 (N+1 Query 해결)
"""

import logging
from typing import Any, Dict, List, Optional

from .base import BaseTerminusService
from .database import DatabaseService
from oms.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class InstanceService(BaseTerminusService):
    """
    TerminusDB 인스턴스 관리 서비스
    
    N+1 Query 문제를 해결하는 최적화된 인스턴스 조회 기능을 제공합니다.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # DatabaseService 인스턴스 (데이터베이스 존재 확인용)
        self.db_service = DatabaseService(*args, **kwargs)
    
    async def get_class_instances_optimized(
        self,
        db_name: str,
        class_id: str,
        limit: int = 100,
        offset: int = 0,
        search: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        특정 클래스의 인스턴스 목록을 효율적으로 조회
        
        N+1 Query 문제를 방지하기 위해 CONSTRUCT 쿼리를 사용하여
        완전히 조립된 인스턴스 객체들을 반환합니다.
        
        Args:
            db_name: 데이터베이스 이름
            class_id: 클래스 ID
            limit: 최대 결과 수
            offset: 시작 위치
            search: 검색어 (선택사항)
            
        Returns:
            완전히 조립된 인스턴스 목록과 총 개수
        """
        try:
            await self.db_service.ensure_db_exists(db_name)
            
            # CONSTRUCT 쿼리로 한 번에 모든 데이터 가져오기
            if search:
                # 검색어가 있는 경우
                search_escaped = search.replace('"', '\\"').replace("'", "\\'")
                sparql_query = f"""
                CONSTRUCT {{
                    ?instance ?property ?value .
                }}
                WHERE {{
                    ?instance a <{class_id}> .
                    ?instance ?property ?value .
                    FILTER(
                        isLiteral(?value) && 
                        regex(str(?value), "{search_escaped}", "i")
                    )
                }}
                LIMIT {limit}
                OFFSET {offset}
                """
            else:
                # 전체 조회
                sparql_query = f"""
                CONSTRUCT {{
                    ?instance ?property ?value .
                }}
                WHERE {{
                    ?instance a <{class_id}> .
                    ?instance ?property ?value .
                }}
                LIMIT {limit}
                OFFSET {offset}
                """
            
            # SPARQL 실행
            endpoint = f"/api/sparql/{self.connection_info.account}/{db_name}"
            result = await self._make_request("POST", endpoint, {"query": sparql_query})
            
            # 결과를 인스턴스별로 그룹화
            instances_map = {}
            
            if isinstance(result, list):
                for triple in result:
                    subject = triple.get("subject", triple.get("@subject"))
                    predicate = triple.get("predicate", triple.get("@predicate"))
                    obj = triple.get("object", triple.get("@object"))
                    
                    if subject and predicate and obj:
                        # 인스턴스 ID 추출
                        instance_id = subject.split("/")[-1] if "/" in subject else subject
                        
                        if instance_id not in instances_map:
                            instances_map[instance_id] = {
                                "instance_id": instance_id,
                                "class_id": class_id,
                                "@type": class_id
                            }
                        
                        # 속성 이름 추출
                        prop_name = predicate.split("/")[-1] if "/" in predicate else predicate
                        prop_name = prop_name.split("#")[-1] if "#" in prop_name else prop_name
                        
                        # rdf:type은 이미 처리했으므로 스킵
                        if prop_name not in ["type", "rdf:type", "@type"]:
                            # 값 처리
                            if isinstance(obj, dict):
                                value = obj.get("@value", obj.get("value", obj))
                            else:
                                value = obj
                            
                            instances_map[instance_id][prop_name] = value
            
            # 리스트로 변환
            instances = list(instances_map.values())
            
            # 총 개수 조회 (별도 COUNT 쿼리)
            count_query = f"""
            SELECT (COUNT(DISTINCT ?instance) as ?count)
            WHERE {{
                ?instance a <{class_id}> .
            }}
            """
            count_result = await self._make_request("POST", endpoint, {"query": count_query})
            
            total_count = 0
            if isinstance(count_result, dict) and "results" in count_result:
                bindings = count_result.get("results", {}).get("bindings", [])
                if bindings and "count" in bindings[0]:
                    total_count = int(bindings[0]["count"]["value"])
            
            return {
                "instances": instances,
                "total": total_count
            }
            
        except Exception as e:
            logger.error(f"Failed to get optimized class instances: {e}")
            raise DatabaseError(f"인스턴스 목록 조회 실패: {e}")
    
    async def get_instance_optimized(
        self,
        db_name: str,
        instance_id: str,
        class_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        개별 인스턴스를 효율적으로 조회
        
        CONSTRUCT 쿼리를 사용하여 모든 속성을 한 번에 가져옵니다.
        
        Args:
            db_name: 데이터베이스 이름
            instance_id: 인스턴스 ID
            class_id: 클래스 ID (선택사항)
            
        Returns:
            완전한 인스턴스 객체 또는 None
        """
        try:
            await self.db_service.ensure_db_exists(db_name)
            
            # CONSTRUCT 쿼리로 인스턴스의 모든 데이터 가져오기
            if class_id:
                sparql_query = f"""
                CONSTRUCT {{
                    <{instance_id}> ?property ?value .
                }}
                WHERE {{
                    <{instance_id}> a <{class_id}> .
                    <{instance_id}> ?property ?value .
                }}
                """
            else:
                sparql_query = f"""
                CONSTRUCT {{
                    <{instance_id}> ?property ?value .
                }}
                WHERE {{
                    <{instance_id}> ?property ?value .
                }}
                """
            
            # SPARQL 실행
            endpoint = f"/api/sparql/{self.connection_info.account}/{db_name}"
            result = await self._make_request("POST", endpoint, {"query": sparql_query})
            
            if not result:
                return None
            
            # 결과를 단일 인스턴스 객체로 조립
            instance_data = {
                "instance_id": instance_id
            }
            
            if isinstance(result, list):
                for triple in result:
                    predicate = triple.get("predicate", triple.get("@predicate"))
                    obj = triple.get("object", triple.get("@object"))
                    
                    if predicate and obj:
                        # 속성 이름 추출
                        prop_name = predicate.split("/")[-1] if "/" in predicate else predicate
                        prop_name = prop_name.split("#")[-1] if "#" in prop_name else prop_name
                        
                        # 값 처리
                        if isinstance(obj, dict):
                            value = obj.get("@value", obj.get("value", obj))
                        else:
                            value = obj
                        
                        # 특별한 속성 처리
                        if prop_name in ["type", "rdf:type"]:
                            instance_data["class_id"] = value.split("/")[-1] if "/" in value else value
                            instance_data["@type"] = instance_data["class_id"]
                        else:
                            instance_data[prop_name] = value
            
            return instance_data if len(instance_data) > 1 else None
            
        except Exception as e:
            logger.error(f"Failed to get optimized instance: {e}")
            raise DatabaseError(f"인스턴스 조회 실패: {e}")
    
    async def count_class_instances(
        self,
        db_name: str,
        class_id: str
    ) -> int:
        """
        특정 클래스의 인스턴스 개수를 효율적으로 조회
        
        Args:
            db_name: 데이터베이스 이름
            class_id: 클래스 ID
            
        Returns:
            인스턴스 개수
        """
        try:
            await self.db_service.ensure_db_exists(db_name)
            
            sparql_query = f"""
            SELECT (COUNT(DISTINCT ?instance) as ?count)
            WHERE {{
                ?instance a <{class_id}> .
            }}
            """
            
            # SPARQL 실행
            endpoint = f"/api/sparql/{self.connection_info.account}/{db_name}"
            result = await self._make_request("POST", endpoint, {"query": sparql_query})
            
            # 결과에서 count 추출
            if isinstance(result, dict) and "results" in result:
                bindings = result.get("results", {}).get("bindings", [])
                if bindings and "count" in bindings[0]:
                    return int(bindings[0]["count"]["value"])
            
            return 0
            
        except Exception as e:
            logger.error(f"Failed to count class instances: {e}")
            raise DatabaseError(f"인스턴스 개수 조회 실패: {e}")