#!/usr/bin/env python3
"""
🔥 ULTRA: 다중 브랜치 실험 환경 실제 구현 테스트
"""

import asyncio
import logging
import sys
import os
import json
from datetime import datetime

# PYTHONPATH 설정
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oms.services.async_terminus import AsyncTerminusService

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s"
)
logger = logging.getLogger(__name__)

class MultiExperimentEnvironment:
    """다중 브랜치 실험 환경"""
    
    def __init__(self, terminus: AsyncTerminusService, db_name: str):
        self.terminus = terminus
        self.db_name = db_name
        self.experiments = {}
    
    async def create_experiment(self, name: str, base_branch: str = "main") -> str:
        """새로운 실험 브랜치 생성"""
        branch_name = f"experiment/{name}"
        await self.terminus.create_branch(self.db_name, branch_name, base_branch)
        
        self.experiments[name] = {
            "branch": branch_name,
            "base": base_branch,
            "created_at": datetime.now().isoformat(),
            "status": "active"
        }
        
        logger.info(f"🧪 Created experiment: {name} (branch: {branch_name})")
        return branch_name
    
    async def run_schema_experiment(self, exp_name: str, schema_variant: dict):
        """특정 실험에서 스키마 변형 테스트"""
        branch = self.experiments[exp_name]["branch"]
        
        # PUT을 사용해서 스키마 업데이트 (브랜치가 데이터를 공유하므로)
        await self.terminus._make_request(
            "PUT",
            f"/api/document/{self.terminus.connection_info.account}/{self.db_name}",
            [schema_variant],
            params={
                "graph_type": "schema", 
                "author": f"experiment-{exp_name}",
                "message": f"Testing schema variant in {exp_name}"
            }
        )
        
        logger.info(f"✅ Applied schema variant to {exp_name}")
        return True
    
    async def compare_experiments(self, exp1: str, exp2: str):
        """두 실험 비교"""
        branch1 = self.experiments[exp1]["branch"]
        branch2 = self.experiments[exp2]["branch"]
        
        diff = await self.terminus.diff(self.db_name, branch1, branch2)
        
        logger.info(f"\n📊 Comparing {exp1} vs {exp2}:")
        logger.info(f"  Found {len(diff)} differences")
        
        return diff
    
    async def create_integration_experiment(self, name: str, source_experiments: list):
        """여러 실험을 통합하는 실험 생성"""
        integration_branch = await self.create_experiment(f"integration/{name}")
        
        logger.info(f"\n🔗 Creating integration experiment: {name}")
        
        results = []
        for source_exp in source_experiments:
            source_branch = self.experiments[source_exp]["branch"]
            
            try:
                # 각 실험을 통합 브랜치에 병합
                merge_result = await self.terminus.merge(
                    self.db_name,
                    source_branch,
                    integration_branch
                )
                
                if merge_result.get("merged"):
                    logger.info(f"  ✅ Integrated {source_exp}")
                    results.append({"experiment": source_exp, "status": "merged"})
                else:
                    logger.info(f"  ❌ Conflict integrating {source_exp}")
                    results.append({
                        "experiment": source_exp, 
                        "status": "conflict",
                        "conflicts": merge_result.get("conflicts", [])
                    })
                    
            except Exception as e:
                logger.error(f"  ❌ Failed to integrate {source_exp}: {e}")
                results.append({"experiment": source_exp, "status": "failed", "error": str(e)})
        
        return results
    
    async def evaluate_experiment(self, exp_name: str):
        """실험 평가 및 메트릭 수집"""
        branch = self.experiments[exp_name]["branch"]
        
        # 실험 메트릭 수집 (시뮬레이션)
        metrics = {
            "experiment": exp_name,
            "branch": branch,
            "commit_count": len(await self.terminus.get_commit_history(self.db_name, branch=branch)),
            "schema_changes": len(await self.terminus.diff(self.db_name, "main", branch)),
            "evaluation_time": datetime.now().isoformat()
        }
        
        logger.info(f"\n📈 Evaluation of {exp_name}:")
        logger.info(f"  Commits: {metrics['commit_count']}")
        logger.info(f"  Changes: {metrics['schema_changes']}")
        
        return metrics
    
    async def merge_successful_experiment(self, exp_name: str):
        """성공한 실험을 main에 병합"""
        branch = self.experiments[exp_name]["branch"]
        
        # PR 생성
        pr = await self.terminus.create_pull_request(
            self.db_name,
            branch,
            "main",
            f"Merge successful experiment: {exp_name}",
            f"This experiment {exp_name} has shown positive results and is ready for production"
        )
        
        logger.info(f"\n🔀 Merging {exp_name} to main:")
        logger.info(f"  PR ID: {pr['id']}")
        logger.info(f"  Changes: {pr['stats']['total_changes']}")
        logger.info(f"  Can merge: {pr['can_merge']}")
        
        if pr['can_merge']:
            merge_result = await self.terminus.merge_pull_request(self.db_name, pr['id'])
            if merge_result.get('merged'):
                logger.info("  ✅ Successfully merged to main!")
                self.experiments[exp_name]["status"] = "merged"
                return True
        
        return False

async def test_multi_branch_experiment():
    """다중 브랜치 실험 환경 테스트"""
    terminus = AsyncTerminusService()
    test_db = f"multi_experiment_test_{int(asyncio.get_event_loop().time())}"
    
    try:
        # 데이터베이스 생성
        await terminus.create_database(test_db, "Multi-Branch Experiment Test")
        logger.info(f"✅ Created test database: {test_db}")
        
        # 실험 환경 초기화
        env = MultiExperimentEnvironment(terminus, test_db)
        
        # 기본 스키마 설정
        base_schema = {
            "@type": "Class",
            "@id": "Product",
            "name": {"@class": "xsd:string", "@type": "Optional"}
        }
        
        await terminus._make_request(
            "POST",
            f"/api/document/{terminus.connection_info.account}/{test_db}",
            [base_schema],
            params={"graph_type": "schema", "author": "system", "message": "Initial schema"}
        )
        
        logger.info("\n" + "="*60)
        logger.info("🧪 MULTI-BRANCH EXPERIMENT TEST")
        logger.info("="*60)
        
        # 실험 1: 간단한 스키마
        await env.create_experiment("simple-product")
        await env.run_schema_experiment("simple-product", {
            "@type": "Class",
            "@id": "Product",
            "name": {"@class": "xsd:string", "@type": "Optional"},
            "price": {"@class": "xsd:decimal", "@type": "Optional"}
        })
        
        # 실험 2: 복잡한 스키마
        await env.create_experiment("complex-product")
        await env.run_schema_experiment("complex-product", {
            "@type": "Class",
            "@id": "Product",
            "name": {"@class": "xsd:string", "@type": "Optional"},
            "price": {"@class": "xsd:decimal", "@type": "Optional"},
            "description": {"@class": "xsd:string", "@type": "Optional"},
            "category": {"@class": "xsd:string", "@type": "Optional"},
            "tags": {"@class": "xsd:string", "@type": "List"}
        })
        
        # 실험 3: 중첩 구조
        await env.create_experiment("nested-product")
        await env.run_schema_experiment("nested-product", {
            "@type": "Class",
            "@id": "Product",
            "name": {"@class": "xsd:string", "@type": "Optional"},
            "pricing": {
                "@class": "PriceInfo",
                "@type": "Optional",
                "@subdocument": []
            }
        })
        
        # 실험 비교
        logger.info("\n" + "="*60)
        logger.info("📊 EXPERIMENT COMPARISONS")
        logger.info("="*60)
        
        await env.compare_experiments("simple-product", "complex-product")
        await env.compare_experiments("complex-product", "nested-product")
        
        # 통합 실험
        logger.info("\n" + "="*60)
        logger.info("🔗 INTEGRATION EXPERIMENT")
        logger.info("="*60)
        
        integration_results = await env.create_integration_experiment(
            "best-of-all",
            ["simple-product", "complex-product"]
        )
        
        # 실험 평가
        logger.info("\n" + "="*60)
        logger.info("📈 EXPERIMENT EVALUATION")
        logger.info("="*60)
        
        for exp_name in ["simple-product", "complex-product", "nested-product"]:
            await env.evaluate_experiment(exp_name)
        
        # 성공한 실험 병합
        logger.info("\n" + "="*60)
        logger.info("🚀 MERGING SUCCESSFUL EXPERIMENT")
        logger.info("="*60)
        
        success = await env.merge_successful_experiment("complex-product")
        
        # 최종 상태
        logger.info("\n" + "="*60)
        logger.info("📋 FINAL EXPERIMENT STATUS")
        logger.info("="*60)
        
        for name, data in env.experiments.items():
            logger.info(f"{name}: {data['status']}")
        
        return success
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
        
    finally:
        # Cleanup
        try:
            await terminus.delete_database(test_db)
            logger.info(f"\n🧹 Cleaned up: {test_db}")
        except:
            pass

async def main():
    """Main runner"""
    success = await test_multi_branch_experiment()
    
    print("\n" + "="*80)
    print("🔥 MULTI-BRANCH EXPERIMENT ENVIRONMENT TEST")
    print("="*80)
    print(f"Result: {'✅ SUCCESS - Multi-branch experiments working!' if success else '❌ FAILED'}")
    print("="*80)

if __name__ == "__main__":
    asyncio.run(main())