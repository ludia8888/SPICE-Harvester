"""
🔥 THINK ULTRA! Type Inference Integration Test
Google Sheets와 Funnel 서비스 통합 테스트
"""

import asyncio
import json
from typing import Dict, Any

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from data_connector.google_sheets.service import GoogleSheetsService
from funnel.services.type_inference import FunnelTypeInferenceService


async def test_google_sheets_type_inference():
    """Google Sheets 데이터로 타입 추론 테스트"""
    
    # Google Sheets 서비스 초기화 (API 키 포함)
    api_key = "AIzaSyAVB9eZPQd57rP3Ta_Uesz-vEptjI0Zj2U"
    sheets_service = GoogleSheetsService()
    sheets_service.api_key = api_key
    
    # 사용자가 제공한 Google Sheets URL
    sheet_url = "https://docs.google.com/spreadsheets/d/1dniTdsPGWbah3NY_m3sMpuyYCR0UVbCYl9TaZJAvZEw/edit?gid=46521583#gid=46521583"
    
    print("🔥 THINK ULTRA! Type Inference Integration Test")
    print("=" * 60)
    print(f"Testing with Google Sheet: {sheet_url}")
    print()
    
    try:
        # 1. Google Sheets 데이터 가져오기
        print("📊 Fetching Google Sheets data...")
        preview_result = await sheets_service.preview_sheet(sheet_url)
        
        print(f"✅ Sheet Title: {preview_result.sheet_title}")
        print(f"✅ Worksheet: {preview_result.worksheet_title}")
        print(f"✅ Total Rows: {preview_result.total_rows}")
        print(f"✅ Columns ({len(preview_result.columns)}): {', '.join(preview_result.columns[:5])}...")
        print()
        
        # 2. Funnel 서비스로 타입 추론
        print("🧠 Analyzing column types...")
        type_inference = FunnelTypeInferenceService()
        analysis_results = type_inference.analyze_dataset(
            data=preview_result.sample_rows,
            columns=preview_result.columns,
            include_complex_types=False  # MVP: 기본 타입만
        )
        
        # 3. 결과 출력
        print("\n📊 Type Inference Results:")
        print("=" * 60)
        
        for i, result in enumerate(analysis_results[:10]):  # 처음 10개 컬럼만 출력
            print(f"\n{i+1}. Column: '{result.column_name}'")
            print(f"   - Inferred Type: {result.inferred_type.type}")
            print(f"   - Confidence: {result.inferred_type.confidence:.0%}")
            print(f"   - Reason: {result.inferred_type.reason}")
            print(f"   - Sample Values: {result.sample_values[:3]}")
            print(f"   - Null Count: {result.null_count}")
            print(f"   - Unique Count: {result.unique_count}")
        
        # 4. 타입별 통계
        print("\n📈 Type Statistics:")
        print("=" * 60)
        type_counts = {}
        for result in analysis_results:
            data_type = result.inferred_type.type
            type_counts[data_type] = type_counts.get(data_type, 0) + 1
        
        for data_type, count in sorted(type_counts.items()):
            percentage = (count / len(analysis_results)) * 100
            print(f"  - {data_type}: {count} columns ({percentage:.1f}%)")
        
        # 5. 신뢰도 분석
        print("\n🎯 Confidence Analysis:")
        print("=" * 60)
        confidence_buckets = {
            "High (90-100%)": 0,
            "Medium (70-89%)": 0,
            "Low (50-69%)": 0,
            "Very Low (<50%)": 0
        }
        
        for result in analysis_results:
            conf = result.inferred_type.confidence
            if conf >= 0.9:
                confidence_buckets["High (90-100%)"] += 1
            elif conf >= 0.7:
                confidence_buckets["Medium (70-89%)"] += 1
            elif conf >= 0.5:
                confidence_buckets["Low (50-69%)"] += 1
            else:
                confidence_buckets["Very Low (<50%)"] += 1
        
        for bucket, count in confidence_buckets.items():
            percentage = (count / len(analysis_results)) * 100
            print(f"  - {bucket}: {count} columns ({percentage:.1f}%)")
        
        # 6. 복합 타입 테스트 (컬럼 이름 힌트 사용)
        print("\n🔍 Testing Complex Type Detection (with column name hints):")
        print("=" * 60)
        
        # 복합 타입 감지 활성화하여 재분석
        complex_results = type_inference.analyze_dataset(
            data=preview_result.sample_rows,
            columns=preview_result.columns,
            include_complex_types=True
        )
        
        # 복합 타입이 감지된 컬럼만 출력
        complex_types = ['custom:email', 'custom:phone', 'custom:money', 'custom:coordinate', 'custom:address']
        complex_detected = []
        
        for result in complex_results:
            if result.inferred_type.type in complex_types:
                complex_detected.append(result)
        
        if complex_detected:
            print(f"\n✨ Found {len(complex_detected)} columns with complex types:")
            for result in complex_detected:
                print(f"  - '{result.column_name}': {result.inferred_type.type} (confidence: {result.inferred_type.confidence:.0%})")
        else:
            print("\n  No complex types detected with current column names.")
        
        # 7. 결과 저장
        results_file = f"type_inference_results_{preview_result.sheet_id}.json"
        results_data = {
            "sheet_info": {
                "sheet_id": preview_result.sheet_id,
                "sheet_title": preview_result.sheet_title,
                "worksheet_title": preview_result.worksheet_title,
                "total_rows": preview_result.total_rows,
                "analyzed_rows": len(preview_result.sample_rows),
                "total_columns": len(preview_result.columns)
            },
            "type_statistics": type_counts,
            "confidence_statistics": confidence_buckets,
            "column_analysis": [
                {
                    "column_name": r.column_name,
                    "inferred_type": r.inferred_type.type,
                    "confidence": r.inferred_type.confidence,
                    "reason": r.inferred_type.reason,
                    "null_count": r.null_count,
                    "unique_count": r.unique_count,
                    "sample_values": r.sample_values[:5]
                }
                for r in analysis_results
            ]
        }
        
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(results_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 Results saved to: {results_file}")
        
        print("\n✅ Integration test completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await sheets_service.close()


async def test_analyze_types_endpoint():
    """Google Sheets /analyze-types 엔드포인트 테스트"""
    import httpx
    from data_connector.google_sheets.models import GoogleSheetPreviewRequest
    
    print("\n🚀 Testing /analyze-types endpoint")
    print("=" * 60)
    
    # 테스트 데이터
    request_data = GoogleSheetPreviewRequest(
        sheet_url="https://docs.google.com/spreadsheets/d/1dniTdsPGWbah3NY_m3sMpuyYCR0UVbCYl9TaZJAvZEw/edit?gid=46521583#gid=46521583",
        api_key="AIzaSyAVB9eZPQd57rP3Ta_Uesz-vEptjI0Zj2U"
    )
    
    async with httpx.AsyncClient() as client:
        try:
            # /analyze-types 엔드포인트 호출
            response = await client.post(
                "http://localhost:8002/api/v1/connectors/google/analyze-types",
                json=request_data.dict(),
                params={"include_complex_types": False}
            )
            
            if response.status_code == 200:
                results = response.json()
                
                print(f"✅ Successfully analyzed {len(results)} columns")
                print("\nFirst 5 columns:")
                
                for i, (col_name, analysis) in enumerate(list(results.items())[:5]):
                    print(f"\n{i+1}. {col_name}:")
                    print(f"   - Type: {analysis['inferred_type']['type']}")
                    print(f"   - Confidence: {analysis['inferred_type']['confidence']:.0%}")
                    print(f"   - Reason: {analysis['inferred_type']['reason']}")
            else:
                print(f"❌ Request failed with status {response.status_code}")
                print(f"   Error: {response.text}")
                
        except httpx.ConnectError:
            print("❌ Could not connect to server. Make sure the BFF service is running on port 8002.")
        except Exception as e:
            print(f"❌ Error: {e}")


async def main():
    """메인 실행 함수"""
    print("🔥 SPICE HARVESTER - Funnel Type Inference Integration Test")
    print("=" * 80)
    
    # 1. 직접 서비스 테스트
    await test_google_sheets_type_inference()
    
    # 2. HTTP 엔드포인트 테스트 (서버가 실행 중인 경우)
    # await test_analyze_types_endpoint()


if __name__ == "__main__":
    asyncio.run(main())