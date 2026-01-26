#!/usr/bin/env python3
"""
완전한 Python 문법 검사 스크립트
모든 백엔드 Python 파일을 체계적으로 검사합니다.
"""

import os
import py_compile
import sys
from pathlib import Path


def check_python_syntax():
    """백엔드 Python 파일들의 문법을 검사합니다."""
    backend_dir = Path(__file__).parent
    
    # 제외할 디렉토리들
    exclude_dirs = {
        'venv', '__pycache__', '.git', '.pytest_cache', 
        'node_modules', '.mypy_cache', '.coverage'
    }
    
    python_files = []
    syntax_errors = []
    successful_compilations = 0
    
    # 모든 Python 파일 찾기
    for root, dirs, files in os.walk(backend_dir):
        # 제외 디렉토리들 필터링
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        
        for file in files:
            if file.endswith('.py'):
                file_path = Path(root) / file
                python_files.append(file_path)
    
    print(f"🔍 총 {len(python_files)}개의 Python 파일을 검사합니다...\n")
    
    # 각 파일 컴파일 테스트
    for file_path in sorted(python_files):
        relative_path = file_path.relative_to(backend_dir)
        try:
            py_compile.compile(str(file_path), doraise=True)
            print(f"✅ {relative_path}")
            successful_compilations += 1
        except py_compile.PyCompileError as e:
            print(f"❌ {relative_path}: {e}")
            syntax_errors.append((relative_path, str(e)))
        except Exception as e:
            print(f"⚠️  {relative_path}: {e}")
            syntax_errors.append((relative_path, str(e)))
    
    # 결과 요약
    print(f"\n{'='*60}")
    print(f"📊 Python 문법 검사 결과:")
    print(f"{'='*60}")
    print(f"✅ 성공: {successful_compilations}개 파일")
    print(f"❌ 오류: {len(syntax_errors)}개 파일")
    print(f"📁 전체: {len(python_files)}개 파일")
    
    if syntax_errors:
        print(f"\n🚨 문법 오류가 있는 파일들:")
        for file_path, error in syntax_errors:
            print(f"   • {file_path}: {error}")
        return False
    else:
        print(f"\n🎉 모든 Python 파일이 정상적으로 컴파일됩니다!")
        return True


if __name__ == "__main__":
    success = check_python_syntax()
    sys.exit(0 if success else 1)