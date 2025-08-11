#!/usr/bin/env python3
"""
TerminusDB 그래프 관계(엣지) 테스트
실제로 관계가 포함된 온톨로지를 생성하고 검증
"""

import asyncio
import json
import logging
from typing import Dict, List, Any
import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GraphRelationshipTester:
    """그래프 관계 테스트 도구"""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.db_name = "graph_test_db"
        self.session = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def create_database(self) -> bool:
        """테스트용 데이터베이스 생성"""
        url = f"{self.base_url}/api/v1/database/create"
        payload = {
            "name": self.db_name,
            "description": "Graph relationship test database"
        }
        
        try:
            async with self.session.post(url, json=payload) as response:
                if response.status in [200, 202]:
                    logger.info(f"✅ Database '{self.db_name}' created")
                    return True
                elif response.status == 409:
                    logger.info(f"ℹ️ Database '{self.db_name}' already exists")
                    return True
                else:
                    error = await response.text()
                    logger.error(f"❌ Failed to create database: {error}")
                    return False
        except Exception as e:
            logger.error(f"❌ Exception creating database: {e}")
            return False
    
    async def create_ontology_with_relationships(self) -> bool:
        """관계가 포함된 온톨로지 생성"""
        
        # 1. Client 온톨로지 (참조 대상)
        client_ontology = {
            "id": "Client",
            "label": "Client",
            "description": "Client entity",
            "properties": [
                {
                    "name": "client_id",
                    "type": "string",
                    "label": "Client ID",
                    "required": True
                },
                {
                    "name": "name",
                    "type": "string",
                    "label": "Client Name",
                    "required": True
                },
                {
                    "name": "country",
                    "type": "string",
                    "label": "Country"
                }
            ]
        }
        
        # 2. Supplier 온톨로지 (참조 대상)
        supplier_ontology = {
            "id": "Supplier",
            "label": "Supplier",
            "description": "Supplier entity",
            "properties": [
                {
                    "name": "supplier_id",
                    "type": "string",
                    "label": "Supplier ID",
                    "required": True
                },
                {
                    "name": "name",
                    "type": "string",
                    "label": "Supplier Name",
                    "required": True
                }
            ]
        }
        
        # 3. Product 온톨로지 (Client와 관계 있음)
        product_ontology = {
            "id": "Product",
            "label": "Product",
            "description": "Product entity with relationships",
            "properties": [
                {
                    "name": "product_id",
                    "type": "string",
                    "label": "Product ID",
                    "required": True
                },
                {
                    "name": "name",
                    "type": "string",
                    "label": "Product Name",
                    "required": True
                },
                {
                    "name": "category",
                    "type": "string",
                    "label": "Category"
                },
                {
                    "name": "unit_price",
                    "type": "decimal",
                    "label": "Unit Price"
                }
            ],
            "relationships": [
                {
                    "predicate": "owned_by",
                    "label": "Owned By",
                    "description": "Product is owned by Client",
                    "target": "Client",
                    "cardinality": "n:1"  # Many products to one client
                }
            ]
        }
        
        # 4. SKU 온톨로지 (Product, Supplier와 관계 있음)
        sku_ontology = {
            "id": "SKU",
            "label": "Stock Keeping Unit",
            "description": "SKU entity with multiple relationships",
            "properties": [
                {
                    "name": "sku_id",
                    "type": "string",
                    "label": "SKU ID",
                    "required": True
                },
                {
                    "name": "color",
                    "type": "string",
                    "label": "Color"
                },
                {
                    "name": "size",
                    "type": "string",
                    "label": "Size"
                },
                {
                    "name": "barcode",
                    "type": "string",
                    "label": "Barcode"
                }
            ],
            "relationships": [
                {
                    "predicate": "belongs_to",
                    "label": "Belongs To",
                    "description": "SKU belongs to Product",
                    "target": "Product",
                    "cardinality": "n:1"
                },
                {
                    "predicate": "supplied_by",
                    "label": "Supplied By",
                    "description": "SKU is supplied by Supplier",
                    "target": "Supplier",
                    "cardinality": "n:1"
                }
            ]
        }
        
        # 5. Order 온톨로지 (Client와 관계 있음)
        order_ontology = {
            "id": "Order",
            "label": "Order",
            "description": "Order entity with relationships",
            "properties": [
                {
                    "name": "order_id",
                    "type": "string",
                    "label": "Order ID",
                    "required": True
                },
                {
                    "name": "order_date",
                    "type": "datetime",
                    "label": "Order Date",
                    "required": True
                },
                {
                    "name": "status",
                    "type": "string",
                    "label": "Status"
                },
                {
                    "name": "total_amount",
                    "type": "decimal",
                    "label": "Total Amount"
                }
            ],
            "relationships": [
                {
                    "predicate": "placed_by",
                    "label": "Placed By",
                    "description": "Order is placed by Client",
                    "target": "Client",
                    "cardinality": "n:1"
                }
            ]
        }
        
        # 6. OrderItem 온톨로지 (Order, SKU와 관계 있음)
        order_item_ontology = {
            "id": "OrderItem",
            "label": "Order Item",
            "description": "Order Item with multiple relationships",
            "properties": [
                {
                    "name": "line_no",
                    "type": "integer",
                    "label": "Line Number",
                    "required": True
                },
                {
                    "name": "quantity",
                    "type": "integer",
                    "label": "Quantity",
                    "required": True
                },
                {
                    "name": "unit_price",
                    "type": "decimal",
                    "label": "Unit Price"
                }
            ],
            "relationships": [
                {
                    "predicate": "item_of",
                    "label": "Item Of",
                    "description": "Item belongs to Order",
                    "target": "Order",
                    "cardinality": "n:1"
                },
                {
                    "predicate": "for_sku",
                    "label": "For SKU",
                    "description": "Item is for specific SKU",
                    "target": "SKU",
                    "cardinality": "n:1"
                }
            ]
        }
        
        # 모든 온톨로지 생성 (의존성 순서대로)
        ontologies = [
            ("Client", client_ontology),
            ("Supplier", supplier_ontology),
            ("Product", product_ontology),
            ("SKU", sku_ontology),
            ("Order", order_ontology),
            ("OrderItem", order_item_ontology)
        ]
        
        success_count = 0
        for name, ontology in ontologies:
            url = f"{self.base_url}/api/v1/ontology/{self.db_name}/create"
            
            try:
                async with self.session.post(url, json=ontology) as response:
                    if response.status in [200, 202]:
                        result = await response.json()
                        success_count += 1
                        
                        # 관계 정보 로깅
                        if "relationships" in ontology:
                            rel_count = len(ontology["relationships"])
                            rel_names = [r["predicate"] for r in ontology["relationships"]]
                            logger.info(f"✅ Created {name} with {rel_count} relationships: {rel_names}")
                        else:
                            logger.info(f"✅ Created {name} (no relationships)")
                    else:
                        error = await response.text()
                        logger.error(f"❌ Failed to create {name}: {error}")
                        
            except Exception as e:
                logger.error(f"❌ Exception creating {name}: {e}")
            
            # API 부하 방지
            await asyncio.sleep(0.5)
        
        logger.info(f"📊 Created {success_count}/{len(ontologies)} ontologies")
        return success_count == len(ontologies)
    
    async def verify_relationships(self) -> bool:
        """생성된 관계 검증"""
        logger.info("\n🔍 Verifying created relationships...")
        
        # 온톨로지 목록 조회
        url = f"{self.base_url}/api/v1/ontology/{self.db_name}/list"
        
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    result = await response.json()
                    ontologies = result.get("data", {}).get("ontologies", [])
                    
                    logger.info(f"Found {len(ontologies)} ontologies")
                    
                    # 각 온톨로지의 관계 확인
                    for ontology in ontologies:
                        ont_id = ontology.get("id")
                        properties = ontology.get("properties", [])
                        
                        # 관계 속성 찾기 (TerminusDB에서는 관계도 properties로 나타남)
                        relationships = []
                        regular_props = []
                        
                        for prop in properties:
                            # 관계는 다른 클래스를 참조하는 속성
                            if prop.get("target") or prop.get("linkTarget"):
                                relationships.append(prop)
                            else:
                                regular_props.append(prop)
                        
                        if relationships:
                            logger.info(f"\n📌 {ont_id}:")
                            logger.info(f"   Properties: {len(regular_props)}")
                            logger.info(f"   Relationships: {len(relationships)}")
                            for rel in relationships:
                                logger.info(f"      • {rel.get('name')} -> {rel.get('target', rel.get('linkTarget'))}")
                        else:
                            logger.info(f"\n📌 {ont_id}: {len(regular_props)} properties, no relationships")
                    
                    return True
                else:
                    error = await response.text()
                    logger.error(f"❌ Failed to list ontologies: {error}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Exception verifying relationships: {e}")
            return False
    
    async def create_test_instances(self) -> bool:
        """관계를 테스트할 인스턴스 생성"""
        logger.info("\n📝 Creating test instances with relationships...")
        
        # 테스트 데이터
        test_data = [
            ("Client", {"client_id": "CL-TEST-001", "name": "Test Client Inc", "country": "USA"}),
            ("Supplier", {"supplier_id": "SUP-TEST-001", "name": "Test Supplier Corp"}),
            ("Product", {"product_id": "PRD-TEST-001", "name": "Test Product", "category": "Electronics", "unit_price": 99.99}),
            ("SKU", {"sku_id": "SKU-TEST-001", "color": "Blue", "size": "Large", "barcode": "123456789"}),
            ("Order", {"order_id": "ORD-TEST-001", "order_date": "2024-08-11T10:00:00Z", "status": "PENDING", "total_amount": 299.97}),
            ("OrderItem", {"line_no": 1, "quantity": 3, "unit_price": 99.99})
        ]
        
        success_count = 0
        for entity_name, data in test_data:
            url = f"{self.base_url}/api/v1/instances/{self.db_name}/async/{entity_name}/create"
            
            try:
                async with self.session.post(url, json={"data": data}) as response:
                    if response.status in [200, 202]:
                        success_count += 1
                        logger.info(f"✅ Created {entity_name} instance")
                    else:
                        error = await response.text()
                        logger.error(f"❌ Failed to create {entity_name}: {error}")
                        
            except Exception as e:
                logger.error(f"❌ Exception creating {entity_name}: {e}")
            
            await asyncio.sleep(0.2)
        
        logger.info(f"📊 Created {success_count}/{len(test_data)} test instances")
        return success_count > 0
    
    async def query_graph_relationships(self) -> bool:
        """그래프 쿼리로 관계 테스트"""
        logger.info("\n🌐 Testing graph queries...")
        
        # WOQL 스타일 쿼리 테스트
        queries = [
            {
                "name": "Products owned by clients",
                "query": "SELECT * FROM Product LIMIT 5"
            },
            {
                "name": "SKUs with relationships",
                "query": "SELECT * FROM SKU LIMIT 5"
            },
            {
                "name": "Order items with order info",
                "query": "SELECT * FROM OrderItem LIMIT 5"
            }
        ]
        
        for q in queries:
            url = f"{self.base_url}/api/v1/query/{self.db_name}"
            
            try:
                async with self.session.get(url, params={"query": q["query"]}) as response:
                    if response.status == 200:
                        result = await response.json()
                        data = result.get("data", [])
                        logger.info(f"✅ {q['name']}: {len(data)} results")
                        if data:
                            logger.info(f"   Sample: {json.dumps(data[0], indent=2)[:200]}...")
                    else:
                        error = await response.text()
                        logger.error(f"❌ Query failed: {error}")
                        
            except Exception as e:
                logger.error(f"❌ Exception in query: {e}")
            
            await asyncio.sleep(0.5)
        
        return True

async def main():
    """메인 실행 함수"""
    logger.info("🚀 Starting TerminusDB Graph Relationship Test...")
    
    async with GraphRelationshipTester() as tester:
        # 1. 데이터베이스 생성
        if not await tester.create_database():
            logger.error("Failed to create database")
            return
        
        # 대기 (Event Sourcing 처리)
        await asyncio.sleep(3)
        
        # 2. 관계가 포함된 온톨로지 생성
        if not await tester.create_ontology_with_relationships():
            logger.error("Failed to create ontologies with relationships")
            return
        
        # 대기
        await asyncio.sleep(5)
        
        # 3. 관계 검증
        await tester.verify_relationships()
        
        # 4. 테스트 인스턴스 생성
        await tester.create_test_instances()
        
        # 대기
        await asyncio.sleep(3)
        
        # 5. 그래프 쿼리 테스트
        await tester.query_graph_relationships()
        
        logger.info("\n✅ Graph relationship test completed!")
        logger.info("📌 Check TerminusDB console to verify the graph structure")

if __name__ == "__main__":
    asyncio.run(main())