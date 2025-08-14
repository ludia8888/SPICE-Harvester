#!/usr/bin/env python3
"""
SPICE HARVESTER 3PL Synthetic Data Injection Tool

실제 합성 데이터 (37K+ 레코드)를 사용하여 SPICE HARVESTER 시스템을 
실사용자 시나리오로 완전 테스트하는 도구

Features:
- 14개 온톨로지 클래스 자동 생성
- 37K+ 레코드 배치 처리 주입
- 실시간 진행상황 모니터링
- Event Sourcing 파이프라인 검증
- 비즈니스 시나리오 테스트 자동화
"""

import asyncio
import csv
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import urllib.request
import urllib.parse
import urllib.error
import aiohttp
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SyntheticDataInjector:
    """3PL 합성 데이터 주입 도구"""
    
    def __init__(self, base_url: str = "http://localhost:8000", data_dir: str = None):
        self.base_url = base_url
        self.data_dir = Path(data_dir) if data_dir else Path("../test_data/spice_harvester_synthetic_3pl")
        self.db_name = "spice_3pl_synthetic"
        self.session: Optional[aiohttp.ClientSession] = None
        
        # 통계 추적
        self.stats = {
            "ontologies_created": 0,
            "instances_created": 0,
            "errors": 0,
            "start_time": None,
            "processing_times": []
        }
        
        # 온톨로지 스키마 정의 (ontology.jsonld 기반)
        self.ontology_schemas = self._load_ontology_schemas()
        
    def _load_ontology_schemas(self) -> Dict[str, Dict]:
        """ontology.jsonld에서 스키마 로드 및 SPICE HARVESTER 형식으로 변환"""
        ontology_file = self.data_dir / "ontology.jsonld"
        
        with open(ontology_file, 'r', encoding='utf-8') as f:
            ontology_data = json.load(f)
        
        schemas = {}
        
        # 각 엔티티를 SPICE HARVESTER 온톨로지 스키마로 변환
        for entity in ontology_data["entities"]:
            entity_id = entity["@id"]
            properties = []
            
            for prop_name in entity["properties"]:
                # CSV 샘플 데이터에서 타입 추론
                prop_type = self._infer_property_type(entity_id, prop_name)
                
                properties.append({
                    "name": prop_name,
                    "type": prop_type,
                    "label": prop_name.replace('_', ' ').title(),
                    "description": f"{entity_id} {prop_name}",
                    "required": prop_name.endswith('_id')  # ID 필드는 필수
                })
            
            schemas[entity_id] = {
                "id": entity_id,
                "label": entity_id,
                "description": f"{entity_id} entity for 3PL synthetic dataset",
                "properties": properties
            }
        
        return schemas
    
    def _infer_property_type(self, entity_id: str, prop_name: str) -> str:
        """CSV 데이터에서 속성 타입 추론"""
        # 기본 타입 매핑
        if prop_name.endswith('_id'):
            return "string"
        elif prop_name.endswith('_date') or prop_name.endswith('_datetime') or prop_name.endswith('_at'):
            return "datetime"
        elif prop_name in ['qty', 'quantity', 'line_no', 'weight_g']:
            return "integer"
        elif prop_name in ['unit_cost', 'unit_price', 'cost_per_unit']:
            return "decimal"
        elif prop_name in ['payload']:
            return "json"
        else:
            return "string"
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def create_all_ontologies(self) -> bool:
        """모든 온톨로지 클래스 생성"""
        logger.info(f"🏗️ Creating {len(self.ontology_schemas)} ontology classes...")
        
        success_count = 0
        for entity_name, schema in self.ontology_schemas.items():
            try:
                success = await self._create_ontology(entity_name, schema)
                if success:
                    success_count += 1
                    self.stats["ontologies_created"] += 1
                    logger.info(f"✅ Created ontology: {entity_name}")
                else:
                    logger.error(f"❌ Failed to create ontology: {entity_name}")
                    self.stats["errors"] += 1
                
                # API 부하 방지를 위한 짧은 대기
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"❌ Error creating ontology {entity_name}: {e}")
                self.stats["errors"] += 1
        
        logger.info(f"🎯 Ontology creation completed: {success_count}/{len(self.ontology_schemas)} successful")
        return success_count == len(self.ontology_schemas)
    
    async def _create_ontology(self, entity_name: str, schema: Dict) -> bool:
        """개별 온톨로지 클래스 생성"""
        url = f"{self.base_url}/api/v1/ontology/{self.db_name}/create"
        
        try:
            async with self.session.post(url, json=schema) as response:
                if response.status == 202:  # Event Sourcing 모드
                    result = await response.json()
                    command_id = result["data"]["command_id"]
                    logger.info(f"📝 Ontology {entity_name} creation command accepted: {command_id}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"❌ Failed to create {entity_name}: {response.status} - {error_text}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Exception creating {entity_name}: {e}")
            return False
    
    async def inject_all_data(self) -> bool:
        """모든 CSV 데이터 주입 (의존성 순서 고려)"""
        logger.info("🚀 Starting massive data injection (37K+ records)...")
        self.stats["start_time"] = time.time()
        
        # 의존성 순서대로 데이터 주입
        injection_order = [
            ("Client", "clients.csv"),
            ("Supplier", "suppliers.csv"), 
            ("Warehouse", "warehouses.csv"),
            ("Location", "locations.csv"),
            ("Product", "products.csv"),
            ("SKU", "skus.csv"),
            ("Inbound", "inbounds.csv"),
            ("Receipt", "receipts.csv"),
            ("Order", "orders.csv"),
            ("OrderItem", "order_items.csv"),
            ("Tracking", "tracking.csv"),
            ("InventorySnapshot", "inventory_snapshots.csv"),
            ("Return", "returns.csv"),
            ("Exception", "exceptions.csv"),
            ("Event", "events.csv"),
            ("Outbox", "outbox.csv")
        ]
        
        total_success = 0
        total_records = 0
        
        for entity_name, csv_file in injection_order:
            logger.info(f"📊 Processing {entity_name} from {csv_file}...")
            
            file_path = self.data_dir / csv_file
            if not file_path.exists():
                logger.warning(f"⚠️ File not found: {csv_file}")
                continue
            
            success, count = await self._inject_csv_data(entity_name, file_path)
            total_success += success
            total_records += count
            
            logger.info(f"✅ {entity_name}: {success}/{count} records injected")
            
            # 각 파일 처리 후 잠시 대기 (시스템 부하 방지)
            await asyncio.sleep(1.0)
        
        # 최종 통계
        elapsed_time = time.time() - self.stats["start_time"]
        logger.info(f"🎯 Data injection completed!")
        logger.info(f"📈 Total: {total_success}/{total_records} records in {elapsed_time:.2f}s")
        logger.info(f"⚡ Rate: {total_success/elapsed_time:.2f} records/sec")
        
        return total_success == total_records
    
    async def _inject_csv_data(self, entity_name: str, file_path: Path) -> Tuple[int, int]:
        """CSV 파일 데이터 배치 주입"""
        try:
            df = pd.read_csv(file_path)
            total_count = len(df)
            success_count = 0
            
            # 배치 크기 (API 부하 고려)
            batch_size = 50
            
            for i in range(0, total_count, batch_size):
                batch = df.iloc[i:i+batch_size]
                batch_success = await self._inject_batch(entity_name, batch)
                success_count += batch_success
                
                # 진행상황 표시
                if i % (batch_size * 10) == 0:
                    progress = (i / total_count) * 100
                    logger.info(f"   Progress: {progress:.1f}% ({i}/{total_count})")
                
                # API 부하 방지
                await asyncio.sleep(0.1)
            
            return success_count, total_count
            
        except Exception as e:
            logger.error(f"❌ Error processing {file_path}: {e}")
            return 0, 0
    
    async def _inject_batch(self, entity_name: str, batch_df: pd.DataFrame) -> int:
        """배치 단위 데이터 주입"""
        success_count = 0
        
        for _, row in batch_df.iterrows():
            try:
                # NaN 값을 None으로 변환
                instance_data = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
                
                success = await self._create_instance(entity_name, instance_data)
                if success:
                    success_count += 1
                    self.stats["instances_created"] += 1
                else:
                    self.stats["errors"] += 1
                    
            except Exception as e:
                logger.error(f"❌ Error injecting instance: {e}")
                self.stats["errors"] += 1
        
        return success_count
    
    async def _create_instance(self, entity_name: str, instance_data: Dict) -> bool:
        """개별 인스턴스 생성 (Event Sourcing)"""
        url = f"{self.base_url}/api/v1/instances/{self.db_name}/async/{entity_name}/create"
        
        payload = {
            "data": instance_data
        }
        
        try:
            async with self.session.post(url, json=payload) as response:
                if response.status == 202:  # Event Sourcing 성공
                    return True
                else:
                    # 실패한 경우 가끔 로그 (너무 많으면 로그 폭증)
                    if self.stats["errors"] % 100 == 0:
                        error_text = await response.text()
                        logger.warning(f"❌ Instance creation failed: {response.status} - {error_text[:200]}")
                    return False
                    
        except Exception as e:
            # 예외도 가끔만 로그
            if self.stats["errors"] % 100 == 0:
                logger.warning(f"❌ Instance creation exception: {e}")
            return False
    
    def print_final_statistics(self):
        """최종 통계 출력"""
        elapsed = time.time() - self.stats["start_time"] if self.stats["start_time"] else 0
        
        print("\n" + "="*60)
        print("🎆 SPICE HARVESTER 3PL Synthetic Data Injection Complete!")
        print("="*60)
        print(f"📊 Ontologies Created: {self.stats['ontologies_created']}")
        print(f"📊 Instances Created: {self.stats['instances_created']}")
        print(f"❌ Errors: {self.stats['errors']}")
        print(f"⏱️ Total Time: {elapsed:.2f} seconds")
        print(f"⚡ Processing Rate: {self.stats['instances_created']/elapsed:.2f} instances/sec")
        print(f"✅ Success Rate: {(self.stats['instances_created']/(self.stats['instances_created']+self.stats['errors'])*100):.2f}%")
        print("="*60)

async def main():
    """메인 실행 함수"""
    logger.info("🚀 SPICE HARVESTER 3PL Synthetic Data Injection Starting...")
    
    # 데이터 디렉토리 확인
    data_dir = "/Users/isihyeon/Desktop/SPICE HARVESTER/test_data/spice_harvester_synthetic_3pl"
    if not Path(data_dir).exists():
        logger.error(f"❌ Data directory not found: {data_dir}")
        return
    
    async with SyntheticDataInjector(data_dir=data_dir) as injector:
        # Phase 1: 온톨로지 생성
        logger.info("🏗️ Phase 1: Creating ontology schemas...")
        ontology_success = await injector.create_all_ontologies()
        
        if not ontology_success:
            logger.error("❌ Ontology creation failed. Stopping.")
            return
        
        # 온톨로지 생성 완료 대기
        logger.info("⏳ Waiting for ontology creation to complete...")
        await asyncio.sleep(10)
        
        # Phase 2: 데이터 주입
        logger.info("📊 Phase 2: Injecting synthetic data...")
        data_success = await injector.inject_all_data()
        
        # 최종 통계
        injector.print_final_statistics()
        
        if ontology_success and data_success:
            logger.info("🎉 All phases completed successfully!")
        else:
            logger.error("❌ Some phases failed. Check logs for details.")

if __name__ == "__main__":
    asyncio.run(main())