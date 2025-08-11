#!/usr/bin/env python3
"""
SPICE HARVESTER 3PL 완전한 그래프 DB 테스트
노드와 엣지를 모두 포함한 실제 그래프 구조 구현
"""

import asyncio
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import aiohttp
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ThreePLGraphBuilder:
    """3PL 그래프 DB 구축 도구"""
    
    def __init__(self):
        self.base_url = "http://localhost:8000"
        self.db_name = "spice_3pl_graph"
        self.data_dir = Path("/Users/isihyeon/Desktop/SPICE HARVESTER/test_data/spice_harvester_synthetic_3pl")
        self.session = None
        self.stats = {
            "ontologies_created": 0,
            "relationships_created": 0,
            "instances_created": 0,
            "start_time": None
        }
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def create_database(self) -> bool:
        """새 데이터베이스 생성"""
        url = f"{self.base_url}/api/v1/database/create"
        payload = {
            "name": self.db_name,
            "description": "3PL Graph Database with full relationships"
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
    
    async def create_ontologies_with_relationships(self) -> bool:
        """모든 온톨로지와 관계 생성"""
        
        # 온톨로지 정의 (의존성 순서)
        ontologies = []
        
        # 1. Client (독립적)
        ontologies.append({
            "id": "Client",
            "label": "Client",
            "description": "3PL Client/Brand",
            "properties": [
                {"name": "client_id", "type": "string", "label": "Client ID", "required": True},
                {"name": "brand_name", "type": "string", "label": "Brand Name", "required": True},
                {"name": "vertical", "type": "string", "label": "Vertical", "required": False}
            ],
            "relationships": []
        })
        
        # 2. Supplier (독립적)
        ontologies.append({
            "id": "Supplier",
            "label": "Supplier",
            "description": "Product Supplier",
            "properties": [
                {"name": "supplier_id", "type": "string", "label": "Supplier ID", "required": True},
                {"name": "name", "type": "string", "label": "Name", "required": True},
                {"name": "country", "type": "string", "label": "Country", "required": False}
            ],
            "relationships": []
        })
        
        # 3. Warehouse (독립적)
        ontologies.append({
            "id": "Warehouse",
            "label": "Warehouse",
            "description": "Warehouse/Fulfillment Center",
            "properties": [
                {"name": "warehouse_id", "type": "string", "label": "Warehouse ID", "required": True},
                {"name": "name", "type": "string", "label": "Name", "required": True},
                {"name": "country", "type": "string", "label": "Country", "required": False},
                {"name": "city", "type": "string", "label": "City", "required": False}
            ],
            "relationships": []
        })
        
        # 4. Product (Client에 의존)
        ontologies.append({
            "id": "Product",
            "label": "Product",
            "description": "Product owned by Client",
            "properties": [
                {"name": "product_id", "type": "string", "label": "Product ID", "required": True},
                {"name": "name", "type": "string", "label": "Name", "required": True},
                {"name": "category", "type": "string", "label": "Category", "required": False},
                {"name": "hs_code", "type": "string", "label": "HS Code", "required": False},
                {"name": "unit_cost", "type": "decimal", "label": "Unit Cost", "required": False},
                {"name": "unit_price", "type": "decimal", "label": "Unit Price", "required": False},
                {"name": "weight_g", "type": "integer", "label": "Weight (g)", "required": False}
            ],
            "relationships": [
                {
                    "predicate": "owned_by",
                    "label": "Owned By",
                    "description": "Product is owned by Client",
                    "target": "Client",
                    "cardinality": "n:1"
                }
            ]
        })
        
        # 5. SKU (Product, Supplier에 의존)
        ontologies.append({
            "id": "SKU",
            "label": "Stock Keeping Unit",
            "description": "SKU belongs to Product and supplied by Supplier",
            "properties": [
                {"name": "sku_id", "type": "string", "label": "SKU ID", "required": True},
                {"name": "color", "type": "string", "label": "Color", "required": False},
                {"name": "size", "type": "string", "label": "Size", "required": False},
                {"name": "barcode", "type": "string", "label": "Barcode", "required": False}
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
        })
        
        # 6. Location (Warehouse에 의존)
        ontologies.append({
            "id": "Location",
            "label": "Location",
            "description": "Location within Warehouse",
            "properties": [
                {"name": "location_id", "type": "string", "label": "Location ID", "required": True},
                {"name": "type", "type": "string", "label": "Type", "required": False}
            ],
            "relationships": [
                {
                    "predicate": "in_warehouse",
                    "label": "In Warehouse",
                    "description": "Location is in Warehouse",
                    "target": "Warehouse",
                    "cardinality": "n:1"
                }
            ]
        })
        
        # 7. Order (Client, Warehouse에 의존)
        ontologies.append({
            "id": "Order",
            "label": "Order",
            "description": "Customer Order",
            "properties": [
                {"name": "order_id", "type": "string", "label": "Order ID", "required": True},
                {"name": "order_datetime", "type": "datetime", "label": "Order DateTime", "required": False},
                {"name": "ship_to_country", "type": "string", "label": "Ship To Country", "required": False},
                {"name": "currency", "type": "string", "label": "Currency", "required": False},
                {"name": "service_level", "type": "string", "label": "Service Level", "required": False},
                {"name": "payment_status", "type": "string", "label": "Payment Status", "required": False}
            ],
            "relationships": [
                {
                    "predicate": "placed_by",
                    "label": "Placed By",
                    "description": "Order is placed by Client",
                    "target": "Client",
                    "cardinality": "n:1"
                },
                {
                    "predicate": "ship_from",
                    "label": "Ship From",
                    "description": "Order ships from Warehouse",
                    "target": "Warehouse",
                    "cardinality": "n:1"
                }
            ]
        })
        
        # 8. OrderItem (Order, SKU에 의존)
        ontologies.append({
            "id": "OrderItem",
            "label": "Order Item",
            "description": "Line item in Order",
            "properties": [
                {"name": "line_no", "type": "integer", "label": "Line Number", "required": True},
                {"name": "qty", "type": "integer", "label": "Quantity", "required": True},
                {"name": "unit_price", "type": "decimal", "label": "Unit Price", "required": False}
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
                    "description": "Item is for SKU",
                    "target": "SKU",
                    "cardinality": "n:1"
                }
            ]
        })
        
        # 9. Inbound (SKU, Supplier, Warehouse에 의존)
        ontologies.append({
            "id": "Inbound",
            "label": "Inbound Shipment",
            "description": "Inbound shipment to warehouse",
            "properties": [
                {"name": "inbound_id", "type": "string", "label": "Inbound ID", "required": True},
                {"name": "eta_date", "type": "date", "label": "ETA Date", "required": False},
                {"name": "arrival_date", "type": "date", "label": "Arrival Date", "required": False},
                {"name": "qty_expected", "type": "integer", "label": "Qty Expected", "required": False},
                {"name": "qty_received", "type": "integer", "label": "Qty Received", "required": False},
                {"name": "cost_per_unit", "type": "decimal", "label": "Cost Per Unit", "required": False}
            ],
            "relationships": [
                {
                    "predicate": "for_sku",
                    "label": "For SKU",
                    "description": "Inbound is for SKU",
                    "target": "SKU",
                    "cardinality": "n:1"
                },
                {
                    "predicate": "from_supplier",
                    "label": "From Supplier",
                    "description": "Inbound from Supplier",
                    "target": "Supplier",
                    "cardinality": "n:1"
                },
                {
                    "predicate": "to_warehouse",
                    "label": "To Warehouse",
                    "description": "Inbound to Warehouse",
                    "target": "Warehouse",
                    "cardinality": "n:1"
                }
            ]
        })
        
        # 10. Receipt (Inbound, SKU, Location에 의존)
        ontologies.append({
            "id": "Receipt",
            "label": "Receipt",
            "description": "Receipt of inbound goods",
            "properties": [
                {"name": "receipt_id", "type": "string", "label": "Receipt ID", "required": True},
                {"name": "qty", "type": "integer", "label": "Quantity", "required": False}
            ],
            "relationships": [
                {
                    "predicate": "of_inbound",
                    "label": "Of Inbound",
                    "description": "Receipt of Inbound",
                    "target": "Inbound",
                    "cardinality": "n:1"
                },
                {
                    "predicate": "for_sku",
                    "label": "For SKU",
                    "description": "Receipt for SKU",
                    "target": "SKU",
                    "cardinality": "n:1"
                },
                {
                    "predicate": "putaway_to",
                    "label": "Putaway To",
                    "description": "Putaway to Location",
                    "target": "Location",
                    "cardinality": "n:1"
                }
            ]
        })
        
        # 11. Tracking (Order에 의존)
        ontologies.append({
            "id": "Tracking",
            "label": "Tracking",
            "description": "Shipment tracking",
            "properties": [
                {"name": "carrier", "type": "string", "label": "Carrier", "required": False},
                {"name": "tracking_number", "type": "string", "label": "Tracking Number", "required": False},
                {"name": "ship_datetime", "type": "datetime", "label": "Ship DateTime", "required": False},
                {"name": "est_delivery_date", "type": "date", "label": "Est Delivery Date", "required": False},
                {"name": "status", "type": "string", "label": "Status", "required": False},
                {"name": "weight_g", "type": "integer", "label": "Weight (g)", "required": False}
            ],
            "relationships": [
                {
                    "predicate": "tracks",
                    "label": "Tracks",
                    "description": "Tracking tracks Order",
                    "target": "Order",
                    "cardinality": "n:1"
                }
            ]
        })
        
        # 12. InventorySnapshot (Warehouse, SKU에 의존)
        ontologies.append({
            "id": "InventorySnapshot",
            "label": "Inventory Snapshot",
            "description": "Inventory count snapshot",
            "properties": [
                {"name": "snapshot_date", "type": "date", "label": "Snapshot Date", "required": True},
                {"name": "qty_counted", "type": "integer", "label": "Qty Counted", "required": False}
            ],
            "relationships": [
                {
                    "predicate": "at_warehouse",
                    "label": "At Warehouse",
                    "description": "Snapshot at Warehouse",
                    "target": "Warehouse",
                    "cardinality": "n:1"
                },
                {
                    "predicate": "of_sku",
                    "label": "Of SKU",
                    "description": "Snapshot of SKU",
                    "target": "SKU",
                    "cardinality": "n:1"
                }
            ]
        })
        
        # 13. Return (Order에 의존)
        ontologies.append({
            "id": "Return",
            "label": "Return",
            "description": "Order return",
            "properties": [
                {"name": "reason", "type": "string", "label": "Reason", "required": False},
                {"name": "created_at", "type": "datetime", "label": "Created At", "required": False}
            ],
            "relationships": [
                {
                    "predicate": "return_of",
                    "label": "Return Of",
                    "description": "Return of Order",
                    "target": "Order",
                    "cardinality": "n:1"
                }
            ]
        })
        
        # 14. Exception (Order에 의존)
        ontologies.append({
            "id": "Exception",
            "label": "Exception",
            "description": "Order exception",
            "properties": [
                {"name": "type", "type": "string", "label": "Type", "required": False},
                {"name": "description", "type": "string", "label": "Description", "required": False},
                {"name": "severity", "type": "string", "label": "Severity", "required": False},
                {"name": "created_at", "type": "datetime", "label": "Created At", "required": False}
            ],
            "relationships": [
                {
                    "predicate": "exception_for",
                    "label": "Exception For",
                    "description": "Exception for Order",
                    "target": "Order",
                    "cardinality": "n:1"
                }
            ]
        })
        
        # 15. Event (자기 참조 가능)
        ontologies.append({
            "id": "Event",
            "label": "Event",
            "description": "System event",
            "properties": [
                {"name": "event_id", "type": "string", "label": "Event ID", "required": True},
                {"name": "event_type", "type": "string", "label": "Event Type", "required": False},
                {"name": "entity_id", "type": "string", "label": "Entity ID", "required": False},
                {"name": "payload", "type": "json", "label": "Payload", "required": False},
                {"name": "created_at", "type": "datetime", "label": "Created At", "required": False}
            ],
            "relationships": [
                {
                    "predicate": "caused_by",
                    "label": "Caused By",
                    "description": "Event caused by another Event",
                    "target": "Event",
                    "cardinality": "n:1"
                }
            ]
        })
        
        # 온톨로지 생성
        success_count = 0
        for ontology in ontologies:
            url = f"{self.base_url}/api/v1/ontology/{self.db_name}/create"
            
            try:
                async with self.session.post(url, json=ontology) as response:
                    if response.status in [200, 202]:
                        result = await response.json()
                        success_count += 1
                        self.stats["ontologies_created"] += 1
                        
                        # 관계 카운트
                        rel_count = len(ontology.get("relationships", []))
                        self.stats["relationships_created"] += rel_count
                        
                        if rel_count > 0:
                            rel_names = [r["predicate"] for r in ontology["relationships"]]
                            logger.info(f"✅ Created {ontology['id']} with {rel_count} relationships: {rel_names}")
                        else:
                            logger.info(f"✅ Created {ontology['id']} (no relationships)")
                    else:
                        error = await response.text()
                        logger.error(f"❌ Failed to create {ontology['id']}: {error}")
                        
            except Exception as e:
                logger.error(f"❌ Exception creating {ontology['id']}: {e}")
            
            await asyncio.sleep(0.5)
        
        logger.info(f"📊 Created {success_count}/{len(ontologies)} ontologies")
        logger.info(f"🔗 Total relationships created: {self.stats['relationships_created']}")
        return success_count == len(ontologies)
    
    async def inject_sample_data(self) -> bool:
        """샘플 데이터 주입 (관계 테스트용)"""
        logger.info("\n📝 Injecting sample data to test relationships...")
        
        # 읽을 CSV 파일들 (제한된 수의 레코드만)
        sample_files = [
            ("Client", "clients.csv", 10),
            ("Supplier", "suppliers.csv", 10),
            ("Warehouse", "warehouses.csv", 5),
            ("Product", "products.csv", 50),
            ("SKU", "skus.csv", 100),
            ("Location", "locations.csv", 20),
            ("Order", "orders.csv", 100),
            ("OrderItem", "order_items.csv", 200),
            ("Inbound", "inbounds.csv", 50),
            ("Receipt", "receipts.csv", 50),
            ("Tracking", "tracking.csv", 100),
            ("InventorySnapshot", "inventory_snapshots.csv", 100),
            ("Return", "returns.csv", 20),
            ("Exception", "exceptions.csv", 20)
        ]
        
        for entity_name, csv_file, limit in sample_files:
            file_path = self.data_dir / csv_file
            if not file_path.exists():
                logger.warning(f"⚠️ File not found: {csv_file}")
                continue
            
            try:
                df = pd.read_csv(file_path, nrows=limit)
                success_count = 0
                
                for _, row in df.iterrows():
                    instance_data = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
                    
                    url = f"{self.base_url}/api/v1/instances/{self.db_name}/async/{entity_name}/create"
                    payload = {"data": instance_data}
                    
                    async with self.session.post(url, json=payload) as response:
                        if response.status in [200, 202]:
                            success_count += 1
                            self.stats["instances_created"] += 1
                    
                    if success_count % 10 == 0:
                        await asyncio.sleep(0.1)
                
                logger.info(f"✅ {entity_name}: {success_count}/{len(df)} records injected")
                
            except Exception as e:
                logger.error(f"❌ Error processing {entity_name}: {e}")
            
            await asyncio.sleep(0.5)
        
        return True
    
    async def test_graph_queries(self) -> None:
        """그래프 관계 테스트 쿼리"""
        logger.info("\n🔍 Testing graph relationships with queries...")
        
        # 테스트 쿼리들
        queries = [
            {
                "name": "Products owned by specific Client",
                "query": """
                    SELECT p.product_id, p.name, p.owned_by as client_id
                    FROM Product p
                    LIMIT 5
                """
            },
            {
                "name": "SKUs with their Product and Supplier",
                "query": """
                    SELECT s.sku_id, s.belongs_to as product_id, s.supplied_by as supplier_id
                    FROM SKU s
                    LIMIT 5
                """
            },
            {
                "name": "Orders with Client and Warehouse",
                "query": """
                    SELECT o.order_id, o.placed_by as client_id, o.ship_from as warehouse_id
                    FROM Order o
                    LIMIT 5
                """
            },
            {
                "name": "OrderItems with Order and SKU relationships",
                "query": """
                    SELECT oi.line_no, oi.item_of as order_id, oi.for_sku as sku_id, oi.qty
                    FROM OrderItem oi
                    LIMIT 5
                """
            }
        ]
        
        for q in queries:
            url = f"{self.base_url}/api/v1/query/{self.db_name}"
            
            try:
                async with self.session.get(url, params={"query": q["query"]}) as response:
                    if response.status == 200:
                        result = await response.json()
                        data = result.get("data", [])
                        logger.info(f"\n✅ {q['name']}: {len(data)} results")
                        if data:
                            # 첫 번째 결과만 출력
                            logger.info(f"   Sample: {json.dumps(data[0], indent=2, default=str)[:300]}...")
                    else:
                        error = await response.text()
                        logger.error(f"❌ Query failed: {error}")
                        
            except Exception as e:
                logger.error(f"❌ Exception in query: {e}")
            
            await asyncio.sleep(0.5)
    
    async def verify_graph_structure(self) -> None:
        """TerminusDB에서 실제 그래프 구조 확인"""
        logger.info("\n🌐 Verifying graph structure in TerminusDB...")
        
        # TerminusDB 직접 접속하여 스키마 확인
        import subprocess
        
        classes_to_check = ["Product", "SKU", "Order", "OrderItem"]
        
        for class_name in classes_to_check:
            schema_url = f"http://localhost:6364/api/document/admin/{self.db_name}?graph_type=schema&id={class_name}"
            
            try:
                result = subprocess.run(
                    ["curl", "-s", "-u", "admin:admin", schema_url],
                    capture_output=True,
                    text=True
                )
                
                if result.stdout:
                    schema = json.loads(result.stdout)
                    logger.info(f"\n📌 {class_name} schema in TerminusDB:")
                    
                    # 관계 찾기
                    relationships = []
                    for key, value in schema.items():
                        if key.startswith("@"):
                            continue
                        
                        if isinstance(value, dict):
                            if "@class" in value and not value["@class"].startswith("xsd:"):
                                # 다른 클래스를 참조하는 관계
                                rel_type = value.get("@type", "direct")
                                target = value.get("@class")
                                relationships.append(f"{key} -> {target} ({rel_type})")
                        elif isinstance(value, str) and not value.startswith("xsd:"):
                            # 직접 참조
                            relationships.append(f"{key} -> {value} (direct)")
                    
                    if relationships:
                        logger.info(f"   🔗 Relationships found:")
                        for rel in relationships:
                            logger.info(f"      • {rel}")
                    else:
                        logger.info(f"   ⚠️ No relationships found")
                    
            except Exception as e:
                logger.error(f"❌ Failed to verify {class_name}: {e}")
        
        logger.info("\n✅ Graph structure verification completed!")

async def main():
    """메인 실행 함수"""
    logger.info("🚀 Starting SPICE HARVESTER 3PL Graph DB Test...")
    logger.info("=" * 60)
    
    async with ThreePLGraphBuilder() as builder:
        builder.stats["start_time"] = time.time()
        
        # 1. 데이터베이스 생성
        if not await builder.create_database():
            logger.error("Failed to create database")
            return
        
        await asyncio.sleep(10)  # 데이터베이스 생성 완료 대기
        
        # 2. 온톨로지와 관계 생성
        if not await builder.create_ontologies_with_relationships():
            logger.error("Failed to create ontologies with relationships")
            return
        
        await asyncio.sleep(5)
        
        # 3. 샘플 데이터 주입
        await builder.inject_sample_data()
        
        await asyncio.sleep(3)
        
        # 4. 그래프 쿼리 테스트
        await builder.test_graph_queries()
        
        # 5. 그래프 구조 검증
        await builder.verify_graph_structure()
        
        # 최종 통계
        elapsed = time.time() - builder.stats["start_time"]
        logger.info("\n" + "=" * 60)
        logger.info("📊 Final Statistics:")
        logger.info(f"   • Ontologies created: {builder.stats['ontologies_created']}")
        logger.info(f"   • Relationships defined: {builder.stats['relationships_created']}")
        logger.info(f"   • Instances created: {builder.stats['instances_created']}")
        logger.info(f"   • Total time: {elapsed:.2f} seconds")
        logger.info(f"   • Processing rate: {builder.stats['instances_created']/elapsed:.2f} instances/sec")
        logger.info("=" * 60)
        logger.info("✅ 3PL Graph DB test completed successfully!")

if __name__ == "__main__":
    asyncio.run(main())