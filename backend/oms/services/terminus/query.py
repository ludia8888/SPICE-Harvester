"""
Query Service for TerminusDB
WOQL 및 SPARQL 쿼리 실행 서비스
"""

import logging
import os
from typing import Any, Dict, List, Optional

from .base import BaseTerminusService
from oms.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class QueryService(BaseTerminusService):
    """
    TerminusDB 쿼리 실행 서비스
    
    WOQL 및 SPARQL 쿼리 실행 기능을 제공합니다.
    """
    
    @staticmethod
    def _normalize_field_name(field: str) -> str:
        if not field:
            return field
        if "/" in field:
            field = field.rsplit("/", 1)[-1]
        if "#" in field:
            field = field.rsplit("#", 1)[-1]
        return field

    @staticmethod
    def _coerce_scalar(value: Any) -> Any:
        if isinstance(value, dict):
            return value.get("@value", value.get("value", value))
        return value

    @staticmethod
    def _as_list(value: Any) -> List[Any]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]

    @staticmethod
    def _try_float(value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str) and value.strip() != "":
                return float(value.strip())
        except Exception:
            return None
        return None

    def _matches_filters(self, doc: Dict[str, Any], filters: List[Dict[str, Any]]) -> bool:
        for f in filters or []:
            if not isinstance(f, dict):
                return False
            field = self._normalize_field_name(str(f.get("field") or ""))
            op = str(f.get("operator") or "").lower()
            raw_expected = f.get("value")

            if not field or not op:
                return False

            if op in {"is_null", "isnull"}:
                if field in doc and doc.get(field) is not None:
                    return False
                continue
            if op in {"is_not_null", "isnotnull"}:
                if field not in doc or doc.get(field) is None:
                    return False
                continue

            actual = self._coerce_scalar(doc.get(field))
            expected = self._coerce_scalar(raw_expected)

            # Normalize lists
            actual_list = self._as_list(actual) if isinstance(actual, list) else None
            expected_list = self._as_list(expected) if isinstance(expected, list) else None

            if op == "eq":
                if actual_list is not None:
                    if expected not in actual_list:
                        return False
                else:
                    if actual != expected:
                        return False
            elif op == "ne":
                if actual_list is not None:
                    if expected in actual_list:
                        return False
                else:
                    if actual == expected:
                        return False
            elif op in {"gt", "ge", "lt", "le"}:
                a_num = self._try_float(actual)
                e_num = self._try_float(expected)
                if a_num is not None and e_num is not None:
                    if op == "gt" and not (a_num > e_num):
                        return False
                    if op == "ge" and not (a_num >= e_num):
                        return False
                    if op == "lt" and not (a_num < e_num):
                        return False
                    if op == "le" and not (a_num <= e_num):
                        return False
                else:
                    a_str = "" if actual is None else str(actual)
                    e_str = "" if expected is None else str(expected)
                    if op == "gt" and not (a_str > e_str):
                        return False
                    if op == "ge" and not (a_str >= e_str):
                        return False
                    if op == "lt" and not (a_str < e_str):
                        return False
                    if op == "le" and not (a_str <= e_str):
                        return False
            elif op == "like":
                a_str = "" if actual is None else str(actual)
                e_str = "" if expected is None else str(expected)
                if e_str.lower() not in a_str.lower():
                    return False
            elif op == "in":
                if expected_list is None:
                    expected_list = self._as_list(expected)
                if actual_list is not None:
                    if not any(v in expected_list for v in actual_list):
                        return False
                else:
                    if actual not in expected_list:
                        return False
            elif op == "not_in":
                if expected_list is None:
                    expected_list = self._as_list(expected)
                if actual_list is not None:
                    if any(v in expected_list for v in actual_list):
                        return False
                else:
                    if actual in expected_list:
                        return False
            else:
                raise DatabaseError(f"Unsupported operator: {op}")

        return True

    async def execute_query(
        self, db_name: str, query_dict: Dict[str, Any], *, branch: str = "main"
    ) -> Dict[str, Any]:
        """
        Execute a query against instance documents.

        Supported input formats:
        1) A simplified query-spec dict produced by OMS/BFF (class_id, filters, select, limit, offset, order_by...)
        2) A raw WOQL dict (when `@type` is present) - executed as-is.
        
        Args:
            db_name: 데이터베이스 이름
            query_dict: query-spec or WOQL dict
            
        Returns:
            {"results": [...], "total": int}
        """
        if not isinstance(query_dict, dict):
            raise DatabaseError("Query payload must be an object")

        # Raw WOQL passthrough (legacy escape hatch).
        if "@type" in query_dict:
            endpoint = f"/api/woql/{self.connection_info.account}/{db_name}{self._branch_descriptor(branch)}"
            woql_request = {
                "query": query_dict,
                "author": self.connection_info.user,
                "message": "WOQL query execution",
            }
            logger.debug(f"Executing WOQL query on database '{db_name}'")
            try:
                result = await self._make_request("POST", endpoint, woql_request)
                if isinstance(result, dict) and "bindings" in result:
                    rows = result["bindings"] or []
                elif isinstance(result, dict) and "results" in result:
                    rows = result["results"] or []
                elif isinstance(result, list):
                    rows = result
                elif isinstance(result, dict):
                    rows = [result]
                else:
                    rows = []
                return {"results": rows, "total": len(rows)}
            except Exception as e:
                logger.error(f"WOQL query execution failed: {e}")
                raise DatabaseError(f"쿼리 실행 실패: {e}")

        # Query-spec path (used by /api/v1/database/{db}/ontology/query)
        class_id = query_dict.get("class_id") or query_dict.get("class_label")
        if not class_id:
            raise DatabaseError("class_id is required")

        try:
            limit = int(query_dict.get("limit") or 50)
        except Exception:
            limit = 50
        try:
            offset = int(query_dict.get("offset") or 0)
        except Exception:
            offset = 0

        if limit < 0:
            limit = 50
        if offset < 0:
            offset = 0

        order_by = query_dict.get("order_by")
        order_direction = str(query_dict.get("order_direction") or "asc").lower()
        reverse = order_direction == "desc"
        filters = query_dict.get("filters") or []
        select = query_dict.get("select")

        endpoint = (
            f"/api/document/{self.connection_info.account}/{db_name}{self._branch_descriptor(branch)}"
        )

        batch_size = min(max(limit + offset, 200), 1000)
        max_scan = int(os.getenv("QUERY_MAX_SCAN_DOCS", "5000"))

        all_matches: List[Dict[str, Any]] = []
        scanned = 0
        skip = 0

        while True:
            params = {
                "type": str(class_id),
                "graph_type": "instance",
                "count": batch_size,
                "skip": skip,
            }
            try:
                result = await self._make_request("GET", endpoint, params=params)
            except Exception as e:
                logger.error(f"Document query failed: {e}")
                raise DatabaseError(f"쿼리 실행 실패: {e}")

            docs: List[Dict[str, Any]] = []
            if result is None or result == "" or (isinstance(result, str) and not result.strip()):
                docs = []
            elif isinstance(result, list):
                docs = [d for d in result if isinstance(d, dict)]
            elif isinstance(result, dict):
                if isinstance(result.get("documents"), list):
                    docs = [d for d in result["documents"] if isinstance(d, dict)]
                elif "@id" in result:
                    docs = [result]
            else:
                docs = []

            if not docs:
                break

            for doc in docs:
                # Normalize a few convenience fields that BFF relies on.
                doc_id_value = doc.get("@id")
                if isinstance(doc_id_value, str):
                    doc["instance_id"] = doc_id_value.split("/")[-1] if "/" in doc_id_value else doc_id_value
                doc["class_id"] = doc.get("@type") or class_id

                if self._matches_filters(doc, filters):
                    all_matches.append(doc)

            skip += len(docs)
            scanned += len(docs)
            if len(docs) < batch_size:
                break
            if scanned >= max_scan:
                logger.warning(
                    f"Query scan cap reached (db={db_name}, class_id={class_id}, scanned={scanned}, max_scan={max_scan}); "
                    "results/total may be partial"
                )
                break

        # Ordering is applied after filtering (simple but deterministic).
        if order_by:
            order_field = self._normalize_field_name(str(order_by))

            def _sort_key(d: Dict[str, Any]) -> tuple[int, Any]:
                v = self._coerce_scalar(d.get(order_field))
                num = self._try_float(v)
                if num is not None:
                    return (0, num)
                if v is None:
                    return (2, "")
                return (1, str(v))

            all_matches.sort(key=_sort_key, reverse=reverse)

        total = len(all_matches)
        if limit == 0:
            page = []
        else:
            page = all_matches[offset : offset + limit]

        if select:
            keep = {self._normalize_field_name(str(s)) for s in select if s}
            keep |= {"@id", "@type", "instance_id", "class_id"}
            page = [{k: v for k, v in doc.items() if k in keep} for doc in page]

        return {"results": page, "total": total}
    
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
