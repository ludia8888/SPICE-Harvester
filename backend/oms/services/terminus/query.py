"""
Query Service for TerminusDB
Foundry-style query-spec execution service
"""

import logging
from typing import Any, Dict, List, Optional

from shared.config.settings import get_settings
from shared.utils.collection_utils import ensure_list

from .base import BaseTerminusService
from oms.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class QueryService(BaseTerminusService):
    """
    TerminusDB 쿼리 실행 서비스

    Foundry-style query-spec 기반 조회 기능을 제공합니다.
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
    def _try_float(value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str) and value.strip() != "":
                return float(value.strip())
        except (TypeError, ValueError):
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
            actual_list = ensure_list(actual) if isinstance(actual, list) else None
            expected_list = ensure_list(expected) if isinstance(expected, list) else None

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
                    expected_list = ensure_list(expected)
                if actual_list is not None:
                    if not any(v in expected_list for v in actual_list):
                        return False
                else:
                    if actual not in expected_list:
                        return False
            elif op == "not_in":
                if expected_list is None:
                    expected_list = ensure_list(expected)
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

        Supported input format:
        - query-spec dict produced by OMS/BFF
          (class_id/class_type, filters, select, limit, offset, order_by...)
        
        Args:
            db_name: 데이터베이스 이름
            query_dict: query-spec dict
            
        Returns:
            {"results": [...], "total": int}
        """
        if not isinstance(query_dict, dict):
            raise DatabaseError("Query payload must be an object")

        if "@type" in query_dict:
            raise DatabaseError("WOQL queries are no longer supported; use query-spec fields")

        # Query-spec path (retained for internal compatibility adapters).
        class_id = query_dict.get("class_id") or query_dict.get("class_type") or query_dict.get("class_label")
        if not class_id:
            raise DatabaseError("class_id is required")

        try:
            limit = int(query_dict.get("limit") or 50)
        except (TypeError, ValueError):
            limit = 50
        try:
            offset = int(query_dict.get("offset") or 0)
        except (TypeError, ValueError):
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
        max_scan = int(get_settings().database.query_max_scan_docs)

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
