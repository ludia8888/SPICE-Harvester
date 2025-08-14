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
        
        TerminusDB Document API를 사용하여 인스턴스를 조회합니다.
        
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
            
            # Document API를 사용하여 인스턴스 조회
            endpoint = f"/api/document/{self.connection_info.account}/{db_name}"
            params = {
                "type": class_id,
                "graph_type": "instance",
                "count": limit,
                "skip": offset
            }
            
            # 검색어가 있는 경우 필터 추가 (TerminusDB는 query 파라미터 지원)
            if search:
                params["query"] = search
            
            # Document API 호출
            result = await self._make_request("GET", endpoint, params=params)
            
            # Handle empty or text responses
            if result is None or result == "" or (isinstance(result, str) and not result.strip()):
                logger.info(f"No instances found for class {class_id} in database {db_name}")
                return {
                    "instances": [],
                    "total": 0,
                    "limit": limit,
                    "offset": offset
                }
            
            # Try to parse if it's a string that looks like JSON
            if isinstance(result, str):
                try:
                    import json
                    result = json.loads(result)
                except json.JSONDecodeError:
                    logger.warning(f"Received non-JSON response: {result[:100]}")
                    return {
                        "instances": [],
                        "total": 0,
                        "limit": limit,
                        "offset": offset
                    }
            
            # 결과 처리
            instances = []
            if isinstance(result, list):
                # 결과가 리스트인 경우 (여러 문서)
                instances = result
            elif isinstance(result, dict):
                # 결과가 단일 문서인 경우
                if "@id" in result:
                    instances = [result]
                else:
                    # 페이징된 결과일 수 있음
                    instances = result.get("documents", [])
            
            # 인스턴스 정리 (불필요한 메타데이터 제거)
            cleaned_instances = []
            for inst in instances:
                if isinstance(inst, dict):
                    # @id에서 인스턴스 ID 추출
                    if "@id" in inst:
                        instance_id = inst["@id"].split("/")[-1] if "/" in inst["@id"] else inst["@id"]
                        inst["instance_id"] = instance_id
                    
                    # class_id 확인/추가
                    if "@type" not in inst:
                        inst["@type"] = class_id
                    inst["class_id"] = class_id
                    
                    cleaned_instances.append(inst)
            
            # 총 개수 조회 (Document API는 별도의 count 엔드포인트가 없으므로 추정)
            # 실제 구현에서는 더 정확한 카운트를 위해 별도 쿼리나 메타데이터 사용
            total_count = len(cleaned_instances)
            if len(cleaned_instances) == limit:
                # 더 많은 결과가 있을 수 있음
                total_count = offset + limit + 1  # 최소 추정치
            
            return {
                "instances": cleaned_instances,
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
            
            # WOQL 실행 (TerminusDB는 SPARQL이 아닌 WOQL 사용)
            endpoint = f"/api/woql/{self.connection_info.account}/{db_name}"
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
            
            # WOQL 실행 (TerminusDB는 SPARQL이 아닌 WOQL 사용)
            endpoint = f"/api/woql/{self.connection_info.account}/{db_name}"
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