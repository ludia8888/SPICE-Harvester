#!/usr/bin/env python3
"""
Production Issues Test
프로덕션 테스트에서 실패하는 문제들 확인
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add backend to Python path
sys.path.insert(0, str(Path(__file__).parent))

from shared.config.service_config import ServiceConfig
from shared.models.config import ConnectionConfig
from oms.services.async_terminus import AsyncTerminusService
from shared.models.ontology import OntologyCreateRequestBFF

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def test_production_failures():
    """프로덕션 테스트에서 실패하는 이슈들 테스트"""
    
    connection_info = ConnectionConfig(
        server_url=ServiceConfig.get_terminus_url(),
        user="admin",
        account="admin", 
        key="admin123"
    )
    
    terminus_service = AsyncTerminusService(connection_info)
    test_db = "production_issues_test"
    
    try:
        await terminus_service.connect()
        
        # 테스트 DB 생성
        try:
            await terminus_service.delete_database(test_db)
        except:
            pass
        
        await terminus_service.create_database(test_db, "Production Issues Test")
        logger.info(f"✅ Created test database: {test_db}")
        
        # === 문제 1: 중복 데이터베이스 방지 (400 대신 200 반환) ===
        print("\n🔍 Testing Issue 1: Duplicate database prevention")
        print("=" * 50)
        
        try:
            # 같은 DB를 다시 생성 시도
            result = await terminus_service.create_database(test_db, "Duplicate Test")
            print(f"❌ Duplicate DB creation succeeded (should fail): {result}")
        except Exception as e:
            if "400" in str(e) or "already exists" in str(e).lower() or "이미 존재합니다" in str(e):
                print("✅ Duplicate DB creation properly rejected")
            else:
                print(f"⚠️ Unexpected error: {e}")
        
        # === 문제 2: 잘못된 프로퍼티 타입 검증 (400 대신 200 반환) ===
        print("\n🔍 Testing Issue 2: Invalid property type validation")
        print("=" * 50)
        
        try:
            # 잘못된 타입으로 온톨로지 생성 시도
            invalid_ontology = {
                "id": "InvalidTypeTest",
                "label": "Invalid Type Test",
                "properties": [
                    {
                        "name": "invalid_prop",
                        "type": "INVALID_TYPE_XYZ",  # 존재하지 않는 타입
                        "label": "Invalid Property"
                    }
                ]
            }
            
            result = await terminus_service.create_ontology_class(test_db, invalid_ontology)
            print(f"❌ Invalid type creation succeeded (should fail): {result}")
            
        except Exception as e:
            if "400" in str(e) or "invalid" in str(e).lower():
                print("✅ Invalid property type properly rejected")
            else:
                print(f"⚠️ Unexpected error: {e}")
        
        # === 문제 3: 브랜치 목록 조회 (빈 배열 반환) ===
        print("\n🔍 Testing Issue 3: List branches functionality")
        print("=" * 50)
        
        try:
            branches = await terminus_service.list_branches(test_db)
            print(f"Current branches: {branches}")
            
            if not branches or len(branches) == 0:
                print("❌ No branches returned (should at least have 'main')")
            elif "main" in branches:
                print("✅ Main branch found in list")
            
            # 새 브랜치 생성 후 다시 확인
            try:
                # 기존 브랜치 삭제 시도 (있을 경우)
                try:
                    await terminus_service.delete_branch(test_db, "test-branch")
                    print("🗑️ Deleted existing test-branch")
                except:
                    pass  # 브랜치가 없으면 무시
                
                await terminus_service.create_branch(test_db, "test-branch", "main")
                print("✅ Created test-branch")
                
                branches_after = await terminus_service.list_branches(test_db)
                print(f"Branches after creation: {branches_after}")
                
                if "test-branch" in branches_after:
                    print("✅ New branch appears in list")
                else:
                    print("❌ New branch not in list")
                    
            except Exception as e:
                print(f"⚠️ Branch creation issue: {e}")
            
        except Exception as e:
            print(f"⚠️ Branch listing issue: {e}")
        
        # === 최종 결과 ===
        print("\n" + "=" * 60)
        print("🎯 PRODUCTION ISSUES SUMMARY")
        print("=" * 60)
        
        print("\nIdentified issues:")
        print("1. Duplicate DB prevention not working (returns 200 instead of 400)")
        print("2. Invalid property type validation not working (returns 200 instead of 400)")
        print("3. Branch listing returns empty array or doesn't update properly")
        
        print("\nThese issues need to be fixed for 100% production test success")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        print(f"\n❌ Production issues test failed: {e}")
    
    finally:
        # 정리
        try:
            await terminus_service.delete_database(test_db)
            await terminus_service.disconnect()
        except:
            pass


async def main():
    """메인 실행"""
    await test_production_failures()


if __name__ == "__main__":
    asyncio.run(main())