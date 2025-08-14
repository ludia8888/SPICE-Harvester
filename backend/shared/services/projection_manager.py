"""
Projection Manager Service
THINK ULTRA³ - 팔란티어 스타일 프로젝션/뷰 Materialization

빈번한 멀티홉 WOQL 쿼리를 주기적으로 실행하여 ES에 캐싱
CQRS 읽기 경로 최적화를 위한 Materialized View 관리
"""

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from shared.services.elasticsearch_service import ElasticsearchService
from shared.services.graph_federation_service_woql import GraphFederationServiceWOQL
from shared.services.redis_service import RedisService

logger = logging.getLogger(__name__)


class ProjectionManager:
    """
    팔란티어 스타일 프로젝션 관리
    
    핵심 원칙:
    1. 빈번한 고비용 멀티홉 = 프로젝션/뷰 materialize
    2. WOQL 정의 질의를 주기/트리거 기반으로 결과를 문서화
    3. 저비용 조회로 전환 (팔란티어 파운드리식 "리파인드 뷰")
    """
    
    def __init__(
        self,
        graph_service: GraphFederationServiceWOQL,
        es_service: ElasticsearchService,
        redis_service: RedisService
    ):
        self.graph_service = graph_service
        self.es_service = es_service
        self.redis_service = redis_service
        
        # 프로젝션 메타데이터 저장용 인덱스
        self.projections_index = "spice_projections_metadata"
        
        # 활성 프로젝션 목록 (메모리 캐시)
        self.active_projections: Dict[str, Dict[str, Any]] = {}
        
        # 프로젝션 실행 태스크
        self.projection_tasks: Dict[str, asyncio.Task] = {}
        
    async def initialize(self):
        """프로젝션 매니저 초기화"""
        # 프로젝션 메타데이터 인덱스 생성
        await self.es_service.create_index_if_not_exists(
            index=self.projections_index,
            mappings={
                "properties": {
                    "projection_id": {"type": "keyword"},
                    "db_name": {"type": "keyword"},
                    "view_name": {"type": "keyword"},
                    "woql_query": {"type": "text"},
                    "query_hash": {"type": "keyword"},
                    "refresh_interval": {"type": "integer"},
                    "last_refresh": {"type": "date"},
                    "next_refresh": {"type": "date"},
                    "status": {"type": "keyword"},
                    "record_count": {"type": "integer"},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"}
                }
            }
        )
        
        # 기존 프로젝션 로드
        await self._load_existing_projections()
        
        logger.info(f"ProjectionManager initialized with {len(self.active_projections)} active projections")
    
    async def register_projection(
        self,
        db_name: str,
        view_name: str,
        start_class: str,
        hops: List[Tuple[str, str]],
        filters: Optional[Dict[str, Any]] = None,
        refresh_interval: int = 3600  # 기본 1시간
    ) -> str:
        """
        새로운 프로젝션 등록
        
        Args:
            db_name: 데이터베이스 이름
            view_name: 프로젝션 뷰 이름
            start_class: 시작 클래스
            hops: 멀티홉 경로
            filters: 필터 조건
            refresh_interval: 갱신 주기 (초)
            
        Returns:
            projection_id: 프로젝션 ID
        """
        # WOQL 쿼리 구성
        woql_config = {
            "start_class": start_class,
            "hops": hops,
            "filters": filters
        }
        
        # 쿼리 해시 생성 (중복 방지)
        query_hash = self._generate_query_hash(db_name, woql_config)
        projection_id = f"{db_name}_{view_name}_{query_hash[:8]}"
        
        # 이미 존재하는 프로젝션인지 확인
        if projection_id in self.active_projections:
            logger.info(f"Projection {projection_id} already exists")
            return projection_id
        
        # 프로젝션 메타데이터 생성
        now = datetime.now(timezone.utc)
        projection_metadata = {
            "projection_id": projection_id,
            "db_name": db_name,
            "view_name": view_name,
            "woql_config": woql_config,
            "query_hash": query_hash,
            "refresh_interval": refresh_interval,
            "last_refresh": None,
            "next_refresh": now,
            "status": "pending",
            "record_count": 0,
            "created_at": now,
            "updated_at": now
        }
        
        # ES에 메타데이터 저장
        await self.es_service.index_document(
            index=self.projections_index,
            doc_id=projection_id,
            document=projection_metadata
        )
        
        # 메모리 캐시 업데이트
        self.active_projections[projection_id] = projection_metadata
        
        # 프로젝션 태스크 시작
        task = asyncio.create_task(self._projection_refresh_loop(projection_id))
        self.projection_tasks[projection_id] = task
        
        logger.info(f"Registered new projection: {projection_id} with {refresh_interval}s refresh interval")
        
        return projection_id
    
    async def materialize_view(
        self,
        projection_id: str,
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        프로젝션 뷰 실체화 (WOQL 실행 → ES 저장)
        
        Args:
            projection_id: 프로젝션 ID
            force_refresh: 강제 갱신 여부
            
        Returns:
            실체화 결과
        """
        if projection_id not in self.active_projections:
            raise ValueError(f"Projection {projection_id} not found")
        
        metadata = self.active_projections[projection_id]
        
        # 강제 갱신이 아니고 캐시가 유효한 경우 스킵
        if not force_refresh and metadata.get("last_refresh"):
            last_refresh = metadata["last_refresh"]
            if isinstance(last_refresh, str):
                last_refresh = datetime.fromisoformat(last_refresh.replace('Z', '+00:00'))
            
            refresh_interval = timedelta(seconds=metadata["refresh_interval"])
            if datetime.now(timezone.utc) - last_refresh < refresh_interval:
                logger.info(f"Projection {projection_id} cache is still valid")
                return {"status": "cached", "record_count": metadata["record_count"]}
        
        # WOQL 쿼리 실행
        logger.info(f"Materializing projection {projection_id}")
        
        woql_config = metadata["woql_config"]
        result = await self.graph_service.multi_hop_query(
            db_name=metadata["db_name"],
            start_class=woql_config["start_class"],
            hops=woql_config["hops"],
            filters=woql_config.get("filters"),
            include_documents=True,
            include_audit=False
        )
        
        # 프로젝션 결과를 ES에 저장
        view_index = f"spice_projection_{metadata['view_name'].lower()}"
        
        # 인덱스 생성 (없으면)
        await self.es_service.create_index_if_not_exists(
            index=view_index,
            mappings={
                "properties": {
                    "projection_id": {"type": "keyword"},
                    "node_id": {"type": "keyword"},
                    "node_type": {"type": "keyword"},
                    "node_data": {"type": "object", "enabled": True},
                    "edges": {"type": "nested"},
                    "materialized_at": {"type": "date"}
                }
            }
        )
        
        # 기존 데이터 삭제
        await self.es_service.delete_by_query(
            index=view_index,
            query={"term": {"projection_id": projection_id}}
        )
        
        # 새 데이터 벌크 인덱싱
        now = datetime.now(timezone.utc)
        documents = []
        
        for node in result["nodes"]:
            doc = {
                "projection_id": projection_id,
                "node_id": node["id"],
                "node_type": node["type"],
                "node_data": node.get("data", {}),
                "edges": [
                    edge for edge in result["edges"] 
                    if edge["from"] == node["id"] or edge["to"] == node["id"]
                ],
                "materialized_at": now
            }
            documents.append(doc)
        
        if documents:
            await self.es_service.bulk_index(
                index=view_index,
                documents=documents
            )
        
        # 메타데이터 업데이트
        metadata["last_refresh"] = now
        metadata["next_refresh"] = now + timedelta(seconds=metadata["refresh_interval"])
        metadata["status"] = "active"
        metadata["record_count"] = len(documents)
        metadata["updated_at"] = now
        
        # ES에 메타데이터 업데이트
        await self.es_service.update_document(
            index=self.projections_index,
            doc_id=projection_id,
            document=metadata
        )
        
        # 메모리 캐시 업데이트
        self.active_projections[projection_id] = metadata
        
        logger.info(f"Materialized projection {projection_id}: {len(documents)} records")
        
        return {
            "status": "materialized",
            "record_count": len(documents),
            "materialized_at": now.isoformat()
        }
    
    async def query_projection(
        self,
        view_name: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        프로젝션 뷰 조회 (캐시된 데이터)
        
        Args:
            view_name: 뷰 이름
            filters: 필터 조건
            limit: 최대 결과 수
            
        Returns:
            프로젝션 결과
        """
        view_index = f"spice_projection_{view_name.lower()}"
        
        # ES 쿼리 구성
        query = {"match_all": {}}
        if filters:
            query = {
                "bool": {
                    "must": [
                        {"term": {f"node_data.{k}": v}}
                        for k, v in filters.items()
                    ]
                }
            }
        
        # ES 조회
        result = await self.es_service.search(
            index=view_index,
            query=query,
            size=limit
        )
        
        # 결과 포맷팅
        documents = []
        for hit in result["hits"]["hits"]:
            doc = hit["_source"]
            documents.append({
                "id": doc["node_id"],
                "type": doc["node_type"],
                "data": doc["node_data"],
                "edges": doc.get("edges", []),
                "materialized_at": doc["materialized_at"]
            })
        
        return documents
    
    async def _projection_refresh_loop(self, projection_id: str):
        """프로젝션 자동 갱신 루프"""
        while projection_id in self.active_projections:
            try:
                metadata = self.active_projections[projection_id]
                
                # 다음 갱신 시간 확인
                next_refresh = metadata.get("next_refresh")
                if isinstance(next_refresh, str):
                    next_refresh = datetime.fromisoformat(next_refresh.replace('Z', '+00:00'))
                
                # 갱신 시간이 되면 실체화
                if datetime.now(timezone.utc) >= next_refresh:
                    await self.materialize_view(projection_id)
                
                # 갱신 주기만큼 대기
                await asyncio.sleep(metadata["refresh_interval"])
                
            except Exception as e:
                logger.error(f"Error in projection refresh loop for {projection_id}: {e}")
                await asyncio.sleep(60)  # 에러 시 1분 대기
    
    async def _load_existing_projections(self):
        """기존 프로젝션 메타데이터 로드"""
        try:
            result = await self.es_service.search(
                index=self.projections_index,
                query={"match_all": {}},
                size=1000
            )
            
            for hit in result["hits"]["hits"]:
                metadata = hit["_source"]
                projection_id = metadata["projection_id"]
                
                # 활성 프로젝션만 로드
                if metadata.get("status") == "active":
                    self.active_projections[projection_id] = metadata
                    
                    # 프로젝션 태스크 재시작
                    task = asyncio.create_task(self._projection_refresh_loop(projection_id))
                    self.projection_tasks[projection_id] = task
                    
            logger.info(f"Loaded {len(self.active_projections)} existing projections")
            
        except Exception as e:
            logger.warning(f"Could not load existing projections: {e}")
    
    def _generate_query_hash(self, db_name: str, woql_config: Dict[str, Any]) -> str:
        """쿼리 설정의 해시 생성"""
        config_str = f"{db_name}:{json.dumps(woql_config, sort_keys=True)}"
        return hashlib.sha256(config_str.encode()).hexdigest()
    
    async def stop_projection(self, projection_id: str):
        """프로젝션 중지"""
        if projection_id in self.projection_tasks:
            self.projection_tasks[projection_id].cancel()
            del self.projection_tasks[projection_id]
        
        if projection_id in self.active_projections:
            metadata = self.active_projections[projection_id]
            metadata["status"] = "stopped"
            metadata["updated_at"] = datetime.now(timezone.utc)
            
            # ES 업데이트
            await self.es_service.update_document(
                index=self.projections_index,
                doc_id=projection_id,
                document=metadata
            )
            
            del self.active_projections[projection_id]
            
        logger.info(f"Stopped projection {projection_id}")
    
    async def shutdown(self):
        """프로젝션 매니저 종료"""
        # 모든 프로젝션 태스크 취소
        for task in self.projection_tasks.values():
            task.cancel()
        
        # 태스크 완료 대기
        await asyncio.gather(*self.projection_tasks.values(), return_exceptions=True)
        
        logger.info("ProjectionManager shutdown complete")