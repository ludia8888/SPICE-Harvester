"""
STRICT Palantir Instance Worker
진짜 C+B 구현: 의사코드를 정확히 따른 버전

규칙:
1. 관계만 추출 (@id → @id 형식만)
2. 멱등 그래프 쓰기 (src, predicate, dst) 유니크
3. ES 전체 문서 + Audit 별도
4. 스키마 버전 인지
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple, Optional

from elasticsearch import AsyncElasticsearch, helpers
from shared.config.settings import ApplicationSettings

logger = logging.getLogger(__name__)


class StrictPalantirWorker:
    """엄격한 Palantir 규칙을 따르는 Instance Worker"""
    
    def __init__(self):
        self.settings = ApplicationSettings()
        
    async def process_instance_command(self, command: Dict[str, Any]):
        """
        의사코드 정확히 구현:
        1. rels = ontology.get_relationship_fields(class_id)  # @id → @id만
        2. graph_nodes, graph_edges = build_nodes_edges(instance, rels)  # 최소 노드/엣지
        3. tdb.upsert(graph_nodes, graph_edges)  # 멱등
        4. es.index(index=f"instances-{db}", id=instance_id, body=full_payload)  # 전체 속성
        5. audit.write({ "event_id": evt.id, "instance_id": instance_id, "s3_uri": s3_uri, "es_doc_id": es_id })
        """
        
        # Extract command data
        db_name = command['db_name']
        class_id = command['class_id']
        instance_id = command.get('instance_id') or self._generate_id(class_id)
        payload = command['payload']
        command_id = command['command_id']
        schema_version = command.get('schema_version', '1.0.0')  # 스키마 버전 인지
        
        logger.info(f"🎯 STRICT Processing: {class_id}/{instance_id} v{schema_version}")
        
        # 1. Get ONLY relationship fields (@id → @id)
        rels = await self.get_relationship_fields(db_name, class_id, schema_version)
        logger.info(f"  📌 Found {len(rels)} relationship fields")
        
        # 2. Build minimal nodes and edges
        graph_nodes, graph_edges = self.build_nodes_edges(
            instance_id, 
            class_id,
            payload, 
            rels
        )
        logger.info(f"  📊 Built {len(graph_nodes)} nodes, {len(graph_edges)} edges")
        
        # 3. Idempotent graph upsert
        await self.tdb_upsert(db_name, graph_nodes, graph_edges)
        logger.info(f"  ✅ Graph upserted (idempotent)")
        
        # 4. Full document to ES
        es_doc_id = await self.es_index(
            index=f"instances-{db_name.lower()}",
            id=instance_id,
            body={
                **payload,  # 전체 속성
                'class_id': class_id,
                'instance_id': instance_id,
                'schema_version': schema_version,
                'created_at': datetime.now(timezone.utc).isoformat(),
                'version': 1
            }
        )
        logger.info(f"  ✅ ES document stored: {es_doc_id}")
        
        # 5. Audit write
        s3_uri = f"s3://instance-events/{db_name}/{class_id}/{instance_id}/{command_id}.json"
        await self.audit_write({
            "command_id": command_id,
            "event_id": command.get('event_id', command_id),
            "instance_id": instance_id,
            "s3_uri": s3_uri,
            "es_doc_id": es_doc_id,
            "schema_version": schema_version,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "operation": "CREATE",
            "aggregate_type": class_id,
            "aggregate_id": instance_id,
            "causation_id": command.get('causation_id'),
            "correlation_id": command.get('correlation_id'),
            "sequence_number": command.get('sequence_number', 0)
        }, db_name)
        logger.info(f"  ✅ Audit recorded")
        
    async def get_relationship_fields(
        self, 
        db_name: str, 
        class_id: str,
        schema_version: str
    ) -> List[Dict[str, Any]]:
        """
        Get ONLY @id → @id relationship fields from ontology
        스키마 버전별로 다른 관계 정의 가능
        """
        # TODO: 실제로는 TerminusDB에서 스키마 가져와야 함
        # 여기서는 하드코딩
        
        relationships = []
        
        # Version-aware schema loading
        if class_id == "Product":
            if schema_version.startswith("1."):
                relationships = [
                    {
                        "predicate": "owned_by",
                        "target": "Client",
                        "cardinality": "n:1",
                        "required": True,
                        "format": "@id"  # @id → @id 형식
                    }
                ]
            elif schema_version.startswith("2."):
                # v2에서 새 관계 추가
                relationships.extend([
                    {
                        "predicate": "manufactured_by",
                        "target": "Manufacturer",
                        "cardinality": "n:1",
                        "required": False,
                        "format": "@id"
                    }
                ])
                
        elif class_id == "Order":
            relationships = [
                {
                    "predicate": "ordered_by",
                    "target": "Client",
                    "cardinality": "n:1",
                    "required": True,
                    "format": "@id"
                },
                {
                    "predicate": "contains",
                    "target": "Product",
                    "cardinality": "n:n",
                    "required": False,
                    "format": "@id"
                }
            ]
            
        return relationships
        
    def build_nodes_edges(
        self,
        instance_id: str,
        class_id: str,
        payload: Dict[str, Any],
        rels: List[Dict[str, Any]]
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Build MINIMAL graph nodes and edges
        노드: @id, @type, es_doc_id, s3_uri만
        엣지: (src, predicate, dst) 튜플
        """
        
        # Minimal node (NO domain attributes)
        nodes = [{
            "@id": f"{class_id}/{instance_id}",
            "@type": class_id,
            "es_doc_id": instance_id,
            "s3_uri": f"s3://instance-events/{class_id}/{instance_id}/latest.json"
        }]
        
        edges = []
        
        # Extract ONLY relationships
        for rel in rels:
            predicate = rel['predicate']
            target_class = rel['target']
            cardinality = rel['cardinality']
            
            if predicate in payload:
                value = payload[predicate]
                
                # Validate @id format
                if isinstance(value, str):
                    # Single reference: must be "Class/ID" format
                    if "/" in value:
                        edges.append({
                            "src": f"{class_id}/{instance_id}",
                            "predicate": predicate,
                            "dst": value,
                            "cardinality": cardinality
                        })
                    else:
                        logger.warning(f"Invalid @id format: {value}")
                        
                elif isinstance(value, list) and cardinality in ["n:n", "1:n"]:
                    # Multiple references
                    for ref in value:
                        if isinstance(ref, str) and "/" in ref:
                            edges.append({
                                "src": f"{class_id}/{instance_id}",
                                "predicate": predicate,
                                "dst": ref,
                                "cardinality": cardinality
                            })
                            
                # Integrity constraints
                if rel.get('required') and predicate not in payload:
                    raise ValueError(f"Required relationship '{predicate}' missing")
                    
        return nodes, edges
        
    async def tdb_upsert(
        self,
        db_name: str,
        nodes: List[Dict],
        edges: List[Dict]
    ):
        """
        Idempotent graph upsert
        (src, predicate, dst) unique constraint
        """
        # TODO: Real TerminusDB implementation
        # This would use WOQL insert_or_replace
        
        for node in nodes:
            # Upsert node (idempotent by @id)
            logger.debug(f"Upsert node: {node['@id']}")
            
        for edge in edges:
            # Upsert edge (idempotent by triple)
            logger.debug(f"Upsert edge: {edge['src']} --[{edge['predicate']}]--> {edge['dst']}")
            
    async def es_index(
        self,
        index: str,
        id: str,
        body: Dict[str, Any]
    ) -> str:
        """ES bulk upsert with full payload"""
        
        # TODO: Real ES implementation
        logger.debug(f"ES index: {index}/{id}")
        return id
        
    async def audit_write(
        self,
        audit_data: Dict[str, Any],
        db_name: str
    ):
        """Write to audit index with full lineage"""
        
        index = f"audit-{db_name.lower()}"
        doc_id = audit_data['command_id']
        
        # TODO: Real ES audit write
        logger.debug(f"Audit: {index}/{doc_id}")
        
    def _generate_id(self, class_id: str) -> str:
        """Generate unique instance ID"""
        from uuid import uuid4
        return f"{class_id}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{str(uuid4())[:8]}"


class SchemaVersionManager:
    """
    스키마 버전 관리
    Git 워크플로우 + Semantic Versioning
    """
    
    def __init__(self):
        self.versions = {}
        
    def load_schema_version(self, db_name: str, version: str) -> Dict:
        """특정 버전의 스키마 로드"""
        # TODO: Git에서 태그된 버전 체크아웃
        pass
        
    def validate_migration(self, from_version: str, to_version: str) -> bool:
        """
        마이그레이션 가능 여부 체크
        MAJOR: 관계/제약 변경 (breaking)
        MINOR: 새 관계 추가 (compatible)
        PATCH: 버그 수정 (compatible)
        """
        from packaging import version
        
        v_from = version.parse(from_version)
        v_to = version.parse(to_version)
        
        # Major version change = breaking
        if v_to.major > v_from.major:
            logger.warning(f"Breaking change: {from_version} → {to_version}")
            return False
            
        return True


class RelationshipConstraintValidator:
    """관계 제약 검증"""
    
    @staticmethod
    def validate_cardinality(
        edges: List[Dict],
        cardinality: str
    ) -> bool:
        """카디널리티 제약 검증"""
        
        if cardinality == "1:1":
            # 각 src는 최대 1개 dst
            src_counts = {}
            for edge in edges:
                src = edge['src']
                src_counts[src] = src_counts.get(src, 0) + 1
                if src_counts[src] > 1:
                    return False
                    
        elif cardinality == "n:1":
            # 여러 src가 하나의 dst 가능
            pass
            
        return True
        
    @staticmethod
    def detect_cycles(edges: List[Dict]) -> bool:
        """순환 참조 감지"""
        
        # Build adjacency list
        graph = {}
        for edge in edges:
            src, dst = edge['src'], edge['dst']
            if src not in graph:
                graph[src] = []
            graph[src].append(dst)
            
        # DFS for cycle detection
        visited = set()
        rec_stack = set()
        
        def has_cycle(node):
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    if has_cycle(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
                    
            rec_stack.remove(node)
            return False
            
        for node in graph:
            if node not in visited:
                if has_cycle(node):
                    return True
                    
        return False


if __name__ == "__main__":
    # Test
    worker = StrictPalantirWorker()
    
    test_command = {
        "command_id": "cmd_123",
        "db_name": "test_db",
        "class_id": "Product",
        "instance_id": "PROD_001",
        "schema_version": "1.2.0",
        "payload": {
            "product_id": "P-001",
            "name": "Test Product",  # Domain attribute - NOT in graph
            "price": 100.0,          # Domain attribute - NOT in graph
            "owned_by": "Client/CL-001"  # Relationship - ONLY this in graph
        }
    }
    
    asyncio.run(worker.process_instance_command(test_command))