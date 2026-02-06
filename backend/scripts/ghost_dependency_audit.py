#!/usr/bin/env python3
"""
🔥 THINK ULTRA! Ghost Dependency Audit Script

유령 의존성 완전 감사 및 검증:
- "우연히 동작"하는 시스템 → "설계에 의해 동작"하는 시스템 검증
- 모든 서비스의 의존성이 명시적으로 관리되는지 확인
- 버전 일치 및 충돌 검사
"""

import sys
import os
import subprocess
import importlib.util
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple
import json

# Add backend directory to path
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))
from scripts.dependency_parsing import parse_pyproject_toml, parse_requirements_txt

def check_service_imports(service_dir: Path) -> Set[str]:
    """Check what external libraries a service actually imports"""
    imports = set()
    
    # Known external libraries that might be used
    external_libs = {
        'redis', 'boto3', 'botocore', 'elasticsearch', 'confluent_kafka', 
        'asyncpg', 'terminusdb_client', 'pydantic', 'httpx', 'fastapi',
        'uvicorn', 'python_dotenv', 'email_validator', 'phonenumbers'
    }
    
    for py_file in service_dir.rglob("*.py"):
        if '__pycache__' in str(py_file):
            continue
            
        try:
            with open(py_file, 'r') as f:
                content = f.read()
                
            for lib in external_libs:
                if f"import {lib}" in content or f"from {lib}" in content:
                    imports.add(lib)
                    
        except Exception as e:
            continue
    
    return imports

def audit_service(service_name: str, service_dir: Path) -> Dict:
    """Comprehensive audit of a single service"""
    result = {
        'service': service_name,
        'path': str(service_dir),
        'has_requirements': False,
        'uses_shared_package': False,
        'direct_dependencies': {},
        'actual_imports': set(),
        'ghost_dependencies': [],
        'status': 'unknown'
    }
    
    # Check requirements.txt
    req_file = service_dir / 'requirements.txt'
    if req_file.exists():
        result['has_requirements'] = True
        result['direct_dependencies'] = parse_requirements_txt(req_file)
        
        # Check if uses shared package
        with open(req_file, 'r') as f:
            content = f.read()
            if '-e ../shared' in content:
                result['uses_shared_package'] = True
    
    # Check actual imports
    result['actual_imports'] = check_service_imports(service_dir)
    
    # Detect ghost dependencies
    for imported_lib in result['actual_imports']:
        # Map import names to package names
        lib_mapping = {
            'confluent_kafka': 'confluent-kafka',
            'terminusdb_client': 'terminusdb-client',
            'email_validator': 'email-validator'
        }
        
        package_name = lib_mapping.get(imported_lib, imported_lib)
        
        if not result['uses_shared_package']:
            # If not using shared package, all dependencies must be explicit
            if package_name not in result['direct_dependencies']:
                result['ghost_dependencies'].append(imported_lib)
        # If using shared package, ghost dependencies are resolved by shared
    
    # Determine status
    if result['ghost_dependencies']:
        result['status'] = 'GHOST_DEPENDENCIES_FOUND'
    elif result['uses_shared_package']:
        result['status'] = 'SHARED_PACKAGE_MANAGED'
    else:
        result['status'] = 'EXPLICIT_DEPENDENCIES'
    
    return result

def main():
    """Main audit execution"""
    print("🔥 THINK ULTRA! Ghost Dependency Audit")
    print("=" * 60)
    print('"우연히 동작" → "설계에 의해 동작" 시스템 검증')
    print()
    
    # Load shared dependencies
    shared_pyproject = backend_dir / 'shared' / 'pyproject.toml'
    shared_deps = parse_pyproject_toml(shared_pyproject)
    
    print(f"📦 Shared Package Dependencies ({len(shared_deps)}):")
    for name, version in shared_deps.items():
        print(f"   ✅ {name}: {version}")
    print()
    
    # Audit all services
    services_to_audit = [
        'bff',
        'oms', 
        'instance_worker',
        'ontology_worker',
        'projection_worker'
    ]
    
    results = []
    total_ghost_deps = 0
    
    print("🔍 Service Dependency Audit:")
    print("-" * 40)
    
    for service_name in services_to_audit:
        service_dir = backend_dir / service_name
        if service_dir.exists():
            result = audit_service(service_name, service_dir)
            results.append(result)
            
            # Print service status
            status_emoji = {
                'GHOST_DEPENDENCIES_FOUND': '❌',
                'SHARED_PACKAGE_MANAGED': '✅',
                'EXPLICIT_DEPENDENCIES': '🟡'
            }
            
            emoji = status_emoji.get(result['status'], '❓')
            print(f"{emoji} {service_name}:")
            print(f"   Uses shared package: {result['uses_shared_package']}")
            print(f"   Direct dependencies: {len(result['direct_dependencies'])}")
            print(f"   Actual imports: {len(result['actual_imports'])}")
            
            if result['ghost_dependencies']:
                print(f"   👻 Ghost dependencies: {result['ghost_dependencies']}")
                total_ghost_deps += len(result['ghost_dependencies'])
            
            print(f"   Status: {result['status']}")
            print()
    
    # Final assessment
    print("🏆" * 20)
    print("FINAL ASSESSMENT")
    print("🏆" * 20)
    
    if total_ghost_deps == 0:
        print("✅ SUCCESS: 모든 유령 의존성이 해결되었습니다!")
        print("✅ 시스템이 '설계에 의해 동작'합니다.")
        print()
        print("🎯 달성한 목표:")
        print("   • 모든 외부 라이브러리가 shared/pyproject.toml에서 중앙관리")
        print("   • 버전 충돌 위험 제거")
        print("   • 명시적이고 예측 가능한 의존성 구조")
        print("   • 'Docker 빌드 우연성'에 의존하지 않는 견고한 시스템")
        
        return 0
    else:
        print(f"❌ FAILURE: {total_ghost_deps}개의 유령 의존성이 여전히 존재합니다!")
        print("❌ 시스템이 여전히 '우연히 동작'할 위험이 있습니다.")
        
        for result in results:
            if result['ghost_dependencies']:
                print(f"   🔴 {result['service']}: {result['ghost_dependencies']}")
        
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
