"""
Query Service for TerminusDB
WOQL 및 SPARQL 쿼리 실행 서비스
"""

import logging
from typing import Any, Dict, List, Optional

from .base import BaseTerminusService
from oms.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class QueryService(BaseTerminusService):
    """
    TerminusDB 쿼리 실행 서비스
    
    WOQL 및 SPARQL 쿼리 실행 기능을 제공합니다.
    """
    
    async def execute_query(self, db_name: str, query_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        WOQL 쿼리 실행
        
        Args:
            db_name: 데이터베이스 이름
            query_dict: WOQL 쿼리 딕셔너리
            
        Returns:
            쿼리 결과
        """
        # TerminusDB WOQL 쿼리 엔드포인트
        endpoint = f"/api/woql/{self.connection_info.account}/{db_name}"
        
        # WOQL 쿼리를 올바른 형식으로 래핑
        woql_request = {
            "query": query_dict,
            "author": self.connection_info.user,
            "message": "WOQL query execution"
        }
        
        logger.debug(f"Executing WOQL query on database '{db_name}'")
        
        try:
            result = await self._make_request("POST", endpoint, woql_request)
            
            # 결과 파싱
            if isinstance(result, dict):
                # bindings가 있는 경우 (표준 WOQL 응답)
                if "bindings" in result:
                    return result["bindings"]
                # 직접 결과를 반환하는 경우
                elif "results" in result:
                    return result["results"]
                else:
                    return [result]
            elif isinstance(result, list):
                return result
            else:
                return []
                
        except Exception as e:
            logger.error(f"WOQL query execution failed: {e}")
            raise DatabaseError(f"쿼리 실행 실패: {e}")
    
    async def execute_sparql(
        self,
        db_name: str,
        sparql_query: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        SPARQL 쿼리 실행
        
        Args:
            db_name: 데이터베이스 이름
            sparql_query: SPARQL 쿼리 문자열
            limit: 결과 제한 수
            offset: 시작 오프셋
            
        Returns:
            쿼리 결과
        """
        # LIMIT/OFFSET 추가 (쿼리에 없는 경우)
        if limit is not None and "LIMIT" not in sparql_query.upper():
            sparql_query += f" LIMIT {limit}"
        if offset is not None and "OFFSET" not in sparql_query.upper():
            sparql_query += f" OFFSET {offset}"
        
        # SPARQL 엔드포인트
        endpoint = f"/api/sparql/{self.connection_info.account}/{db_name}"
        
        # SPARQL 쿼리 요청
        data = {"query": sparql_query}
        
        logger.debug(f"Executing SPARQL query on database '{db_name}'")
        
        try:
            result = await self._make_request("POST", endpoint, data)
            
            # 결과 형식 표준화
            if isinstance(result, dict) and "results" in result:
                # SELECT 쿼리 결과
                bindings = result.get("results", {}).get("bindings", [])
                formatted_results = []
                
                for binding in bindings:
                    row = {}
                    for var, value_obj in binding.items():
                        if isinstance(value_obj, dict) and "value" in value_obj:
                            row[var] = value_obj["value"]
                        else:
                            row[var] = value_obj
                    formatted_results.append(row)
                
                return {
                    "results": formatted_results,
                    "total": len(formatted_results)
                }
            elif isinstance(result, list):
                # CONSTRUCT 쿼리 결과
                return {
                    "results": result,
                    "total": len(result)
                }
            else:
                return {
                    "results": [],
                    "total": 0
                }
                
        except Exception as e:
            logger.error(f"SPARQL query execution failed: {e}")
            raise DatabaseError(f"SPARQL 쿼리 실행 실패: {e}")
    
    def convert_to_woql(self, query_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        간단한 쿼리 딕셔너리를 WOQL 형식으로 변환
        
        Args:
            query_dict: 간단한 쿼리 딕셔너리
            
        Returns:
            WOQL 쿼리
        """
        # 쿼리 타입 확인
        query_type = query_dict.get("type", "select")
        
        if query_type == "select":
            # SELECT 쿼리 변환
            woql_query = {
                "@type": "Select",
                "variables": query_dict.get("variables", ["v:X"]),
                "query": {
                    "@type": "Triple",
                    "subject": query_dict.get("subject", "v:X"),
                    "predicate": query_dict.get("predicate", "@type"),
                    "object": query_dict.get("object", "Class")
                }
            }
        elif query_type == "insert":
            # INSERT 쿼리 변환
            woql_query = {
                "@type": "AddTriple",
                "subject": query_dict["subject"],
                "predicate": query_dict["predicate"],
                "object": query_dict["object"]
            }
        elif query_type == "delete":
            # DELETE 쿼리 변환
            woql_query = {
                "@type": "DeleteTriple",
                "subject": query_dict["subject"],
                "predicate": query_dict.get("predicate", "@type"),
                "object": query_dict.get("object", "@wildcard")
            }
        else:
            # 기타 - 원본 반환
            woql_query = query_dict
        
        # LIMIT 및 OFFSET 추가
        limit = query_dict.get("limit")
        offset = query_dict.get("offset")
        
        if limit and isinstance(limit, int) and limit > 0:
            woql_query = {"@type": "Limit", "limit": limit, "query": woql_query}
        
        if offset and isinstance(offset, int) and offset > 0:
            woql_query = {"@type": "Start", "start": offset, "query": woql_query}
        
        return woql_query