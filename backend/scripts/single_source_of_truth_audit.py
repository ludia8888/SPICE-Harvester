#!/usr/bin/env python3
"""
🎯 THINK ULTRA! Single Source of Truth Verification

MSA 전역 의존성 아키텍처 완전성 검증:
- 단일 진실 공급원(SSoT) 원칙 준수 확인
- 중복된 의존성 선언 탐지
- 버전 일관성 검증
- 의존성 지옥 방지 및 예방
"""

import sys
import os
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple
import re

try:
    import tomllib  # Python 3.11+
except ImportError:
    try:
        import tomli as tomllib  # Fallback for older Python
    except ImportError:
        import toml as tomllib  # Last resort fallback

# Add backend directory to path
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

def parse_requirements_txt(file_path: Path) -> Dict[str, Optional[str]]:
    """Parse requirements.txt file and extract dependencies with versions"""
    deps = {}
    if not file_path.exists():
        return deps
    
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and not line.startswith('-e'):
                # Extract package name and version
                if '==' in line:
                    name, version = line.split('==', 1)
                    deps[name.strip()] = version.strip()
                elif '>=' in line:
                    name, version = line.split('>=', 1)
                    deps[name.strip()] = f">={version.strip()}"
                elif '<=' in line:
                    name, version = line.split('<=', 1)
                    deps[name.strip()] = f"<={version.strip()}"
                else:
                    # No version specified
                    deps[line.strip()] = None
    return deps

def parse_pyproject_toml(file_path: Path) -> Dict[str, str]:
    """Parse pyproject.toml dependencies"""
    deps = {}
    if not file_path.exists():
        return deps
    
    try:
        with open(file_path, 'rb') as f:
            data = tomllib.load(f)
        
        project_deps = data.get('project', {}).get('dependencies', [])
        for dep in project_deps:
            # Skip editable installs
            if dep.startswith('-e '):
                continue
                
            # Extract package name and version
            if '==' in dep:
                name, version = dep.split('==', 1)
                deps[name.strip()] = version.strip()
            elif '>=' in dep:
                name, version = dep.split('>=', 1)
                deps[name.strip()] = f">={version.strip()}"
            elif '<=' in dep:
                name, version = dep.split('<=', 1)
                deps[name.strip()] = f"<={version.strip()}"
            else:
                # Handle special cases like redis[hiredis]
                if '[' in dep:
                    name = dep.split('[')[0]
                    deps[name.strip()] = None
                else:
                    deps[dep.strip()] = None
        
        return deps
    except Exception as e:
        print(f"Error parsing {file_path}: {e}")
        return {}

def check_duplicate_dependencies() -> Tuple[Dict[str, List[str]], int]:
    """Check for duplicate dependency declarations across files"""
    print("🔍 DUPLICATE DEPENDENCY DETECTION")
    print("=" * 50)
    
    # Parse shared/pyproject.toml (should be the single source of truth)
    shared_deps = parse_pyproject_toml(backend_dir / 'shared' / 'pyproject.toml')
    print(f"📦 Shared package manages {len(shared_deps)} dependencies")
    
    # Check all requirements.txt files for duplicates
    duplicates = {}
    total_violations = 0
    
    services_to_check = [
        'bff', 'oms', 'funnel', 'message_relay',
        'instance_worker', 'ontology_worker', 'projection_worker',
        'tests'
    ]
    
    for service in services_to_check:
        req_file = backend_dir / service / 'requirements.txt'
        if req_file.exists():
            service_deps = parse_requirements_txt(req_file)
            
            for dep_name, dep_version in service_deps.items():
                if dep_name in shared_deps:
                    if dep_name not in duplicates:
                        duplicates[dep_name] = []
                    duplicates[dep_name].append(f"{service} ({dep_version})")
                    total_violations += 1
    
    # Check root pyproject.toml for duplicates
    root_deps = parse_pyproject_toml(backend_dir / 'pyproject.toml')
    for dep_name, dep_version in root_deps.items():
        # Skip the shared package reference itself
        if dep_name.startswith('-e '):
            continue
            
        if dep_name in shared_deps:
            if dep_name not in duplicates:
                duplicates[dep_name] = []
            duplicates[dep_name].append(f"root pyproject.toml ({dep_version})")
            total_violations += 1
    
    return duplicates, total_violations

def check_version_consistency() -> Tuple[Dict[str, Dict[str, str]], int]:
    """Check for version inconsistencies across files"""
    print("\n📊 VERSION CONSISTENCY VERIFICATION")
    print("=" * 50)
    
    # Get shared dependencies as the source of truth
    shared_deps = parse_pyproject_toml(backend_dir / 'shared' / 'pyproject.toml')
    
    inconsistencies = {}
    total_inconsistencies = 0
    
    # Check all files that might have version declarations
    files_to_check = [
        ('root pyproject.toml', backend_dir / 'pyproject.toml'),
        ('tests/requirements.txt', backend_dir / 'tests' / 'requirements.txt'),
        ('bff/requirements.txt', backend_dir / 'bff' / 'requirements.txt'),
        ('oms/requirements.txt', backend_dir / 'oms' / 'requirements.txt'),
        ('funnel/requirements.txt', backend_dir / 'funnel' / 'requirements.txt'),
        ('message_relay/requirements.txt', backend_dir / 'message_relay' / 'requirements.txt'),
    ]
    
    for file_name, file_path in files_to_check:
        if file_path.name.endswith('.toml'):
            file_deps = parse_pyproject_toml(file_path)
        else:
            file_deps = parse_requirements_txt(file_path)
        
        for dep_name, dep_version in file_deps.items():
            if dep_name in shared_deps and dep_version != shared_deps[dep_name]:
                if dep_name not in inconsistencies:
                    inconsistencies[dep_name] = {'shared': shared_deps[dep_name]}
                inconsistencies[dep_name][file_name] = dep_version
                total_inconsistencies += 1
    
    return inconsistencies, total_inconsistencies

def check_single_source_compliance() -> bool:
    """Verify that all services use only -e ../shared in requirements.txt"""
    print("\n🎯 SINGLE SOURCE OF TRUTH COMPLIANCE")
    print("=" * 50)
    
    services = ['bff', 'oms', 'funnel', 'message_relay', 'instance_worker', 'ontology_worker', 'projection_worker']
    compliant_services = 0
    non_compliant = []
    
    for service in services:
        req_file = backend_dir / service / 'requirements.txt'
        if req_file.exists():
            with open(req_file, 'r') as f:
                content = f.read()
            
            # Check if it only contains -e ../shared and comments
            lines = [line.strip() for line in content.split('\n') if line.strip()]
            non_comment_lines = [line for line in lines if not line.startswith('#')]
            
            if len(non_comment_lines) == 1 and non_comment_lines[0] == '-e ../shared':
                print(f"   ✅ {service}: Perfect compliance")
                compliant_services += 1
            else:
                non_deps = [line for line in non_comment_lines if line != '-e ../shared']
                if non_deps:
                    print(f"   ❌ {service}: Has additional dependencies: {non_deps}")
                    non_compliant.append(service)
                else:
                    print(f"   ✅ {service}: Compliant")
                    compliant_services += 1
        else:
            print(f"   ⚠️  {service}: No requirements.txt file")
    
    print(f"\n📊 Compliance Summary: {compliant_services}/{len(services)} services compliant")
    return len(non_compliant) == 0

def main():
    """Main audit execution"""
    print("🎯 THINK ULTRA! Single Source of Truth Audit")
    print("=" * 60)
    print('"의존성 지옥" → "단일 진실 공급원" 아키텍처 검증')
    print()
    
    # Check 1: Duplicate Dependencies
    duplicates, dup_violations = check_duplicate_dependencies()
    
    # Check 2: Version Consistency
    inconsistencies, version_violations = check_version_consistency()
    
    # Check 3: Single Source Compliance
    is_compliant = check_single_source_compliance()
    
    # Final Assessment
    print("\n" + "🏆" * 20)
    print("FINAL ASSESSMENT")
    print("🏆" * 20)
    
    total_issues = dup_violations + version_violations + (0 if is_compliant else 1)
    
    if total_issues == 0:
        print("✅ SUCCESS: 완벽한 단일 진실 공급원 아키텍처!")
        print("✅ 모든 의존성이 shared/pyproject.toml에서 중앙관리됩니다.")
        print("✅ 버전 불일치 및 중복 선언이 완전히 제거되었습니다.")
        print()
        print("🎯 달성한 목표:")
        print("   • 의존성 지옥(Dependency Hell) 완전 방지")
        print("   • 단일 진실 공급원(SSoT) 원칙 100% 준수")  
        print("   • MSA 환경에서 예측 가능한 빌드/배포 파이프라인")
        print("   • 개발팀 간 의존성 관리 혼란 제거")
        
        return 0
    else:
        print(f"❌ FAILURE: {total_issues}개의 아키텍처 위반 사항 발견!")
        print()
        
        if duplicates:
            print("🔴 중복된 의존성 선언:")
            for dep, locations in duplicates.items():
                print(f"   • {dep}: {', '.join(locations)}")
        
        if inconsistencies:
            print("🔴 버전 불일치:")
            for dep, versions in inconsistencies.items():
                print(f"   • {dep}:")
                for location, version in versions.items():
                    print(f"     - {location}: {version}")
        
        if not is_compliant:
            print("🔴 단일 진실 공급원 원칙 위반")
        
        print("\n🔧 해결 방안:")
        print("   1. 중복된 의존성을 shared/pyproject.toml로 이동")
        print("   2. 모든 requirements.txt를 '-e ../shared'만 포함하도록 수정")
        print("   3. 버전 불일치를 shared/pyproject.toml 기준으로 통일")
        
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)