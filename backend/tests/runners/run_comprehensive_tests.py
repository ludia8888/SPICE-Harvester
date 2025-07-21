#!/usr/bin/env python3
"""
종합 테스트 실행기
프로덕션 레벨 통합 테스트 + 성능 테스트 + 보고서 생성

think ultra: 모든 테스트를 순차적으로 실행하고 최종 프로덕션 준비도 평가
"""

import asyncio
import sys
import os
import time
import json
import subprocess
from datetime import datetime
from pathlib import Path

# 테스트 모듈 import
from test_comprehensive_production_integration import ProductionIntegrationTestSuite
from test_production_performance_suite import ProductionPerformanceTestSuite

class ComprehensiveTestRunner:
    """종합 테스트 실행기"""
    
    def __init__(self):
        self.test_start_time = datetime.now()
        self.test_results = {
            "integration_tests": {},
            "performance_tests": {},
            "funnel_integration_tests": {},
            "coverage_analysis": {},
            "overall_status": "running",
            "start_time": self.test_start_time.isoformat(),
            "end_time": None,
            "total_duration": 0,
            "production_readiness_score": 0
        }
        self.services_status = {
            "oms": False,
            "bff": False,
            "funnel": False,
            "terminusdb": False
        }
    
    async def check_service_prerequisites(self):
        """서비스 전제 조건 확인"""
        print("🔍 서비스 전제 조건 확인")
        
        # Python 패키지 확인
        required_packages = ["httpx", "psutil", "asyncio", "statistics", "pytest"]
        missing_packages = []
        
        for package in required_packages:
            try:
                __import__(package)
                print(f"  ✅ {package} 패키지 사용 가능")
            except ImportError:
                missing_packages.append(package)
                print(f"  ❌ {package} 패키지 누락")
        
        if missing_packages:
            print(f"\n🚨 누락된 패키지: {missing_packages}")
            print("설치 명령: pip install " + " ".join(missing_packages))
            return False
        
        # 서비스 포트 확인
        import socket
        
        services_to_check = [
            ("OMS", "localhost", 8000),
            ("BFF", "localhost", 8002),
            ("Funnel", "localhost", 8003),
            ("TerminusDB", "localhost", 6363)
        ]
        
        for service_name, host, port in services_to_check:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2)
                result = sock.connect_ex((host, port))
                sock.close()
                
                if result == 0:
                    print(f"  ✅ {service_name} 서비스 ({host}:{port}) 실행 중")
                    if service_name.lower() == "oms":
                        self.services_status["oms"] = True
                    elif service_name.lower() == "bff":
                        self.services_status["bff"] = True
                    elif service_name.lower() == "funnel":
                        self.services_status["funnel"] = True
                    elif service_name.lower() == "terminusdb":
                        self.services_status["terminusdb"] = True
                else:
                    print(f"  ⚠️ {service_name} 서비스 ({host}:{port}) 연결 불가")
                    
            except Exception as e:
                print(f"  ❌ {service_name} 서비스 확인 실패: {e}")
        
        return True
    
    async def run_integration_tests(self):
        """통합 테스트 실행"""
        print("\n" + "="*80)
        print("🔗 프로덕션 레벨 통합 테스트 시작")
        print("="*80)
        
        integration_start_time = time.time()
        
        try:
            integration_suite = ProductionIntegrationTestSuite()
            await integration_suite.run_comprehensive_tests()
            
            # 결과 수집
            self.test_results["integration_tests"] = {
                "status": "completed",
                "test_results": integration_suite.test_results,
                "security_violations": integration_suite.security_violations,
                "duration": time.time() - integration_start_time,
                "success_rate": len([t for t in integration_suite.test_results if t["status"] == "passed"]) / len(integration_suite.test_results) if integration_suite.test_results else 0
            }
            
            print(f"✅ 통합 테스트 완료 ({time.time() - integration_start_time:.1f}초)")
            
        except Exception as e:
            print(f"❌ 통합 테스트 실패: {e}")
            self.test_results["integration_tests"] = {
                "status": "failed",
                "error": str(e),
                "duration": time.time() - integration_start_time
            }
    
    async def run_performance_tests(self):
        """성능 테스트 실행"""
        print("\n" + "="*80)
        print("⚡ 프로덕션 레벨 성능 테스트 시작")
        print("="*80)
        
        performance_start_time = time.time()
        
        try:
            performance_suite = ProductionPerformanceTestSuite()
            await performance_suite.run_comprehensive_performance_tests()
            
            # 결과 수집
            self.test_results["performance_tests"] = {
                "status": "completed",
                "performance_results": performance_suite.performance_results,
                "resource_metrics": performance_suite.resource_metrics,
                "duration": time.time() - performance_start_time,
                "thresholds": performance_suite.thresholds
            }
            
            print(f"✅ 성능 테스트 완료 ({time.time() - performance_start_time:.1f}초)")
            
        except Exception as e:
            print(f"❌ 성능 테스트 실패: {e}")
            self.test_results["performance_tests"] = {
                "status": "failed",
                "error": str(e),
                "duration": time.time() - performance_start_time
            }
    
    def run_coverage_analysis(self):
        """테스트 커버리지 분석 실행"""
        print("\n" + "="*80)
        print("📊 테스트 커버리지 분석 시작")
        print("="*80)
        
        coverage_start_time = time.time()
        
        try:
            # 프로젝트 루트 디렉토리 찾기
            current_dir = Path(__file__).parent.parent.parent
            coverage_script = current_dir / "run_coverage_report.py"
            
            if not coverage_script.exists():
                raise FileNotFoundError(f"Coverage script not found: {coverage_script}")
            
            # 커버리지 분석 실행
            print("🔍 실행 중: 종합 커버리지 분석...")
            
            result = subprocess.run([
                "python3", str(coverage_script),
                "--project-root", str(current_dir),
                "--test-pattern", "tests/",
                "--include-performance"
            ], 
            cwd=current_dir,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
            )
            
            # 결과 파싱
            coverage_data = {
                "status": "completed" if result.returncode == 0 else "failed",
                "return_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "duration": time.time() - coverage_start_time,
                "overall_coverage": 0.0,
                "meets_minimum_threshold": False,
                "grade": "F",
                "reports_generated": []
            }
            
            # stdout에서 커버리지 정보 추출
            if result.stdout:
                lines = result.stdout.split('\n')
                for line in lines:
                    if "Overall Coverage:" in line:
                        # "Overall Coverage: 85.50% (Grade: A) - good" 형식
                        try:
                            parts = line.split()
                            for i, part in enumerate(parts):
                                if part.endswith('%'):
                                    coverage_data["overall_coverage"] = float(part.rstrip('%'))
                                    break
                            if "(Grade:" in line:
                                grade_part = line.split("(Grade:")[1].split(")")[0].strip()
                                coverage_data["grade"] = grade_part
                        except (ValueError, IndexError):
                            pass
                    elif "Meets minimum coverage requirements" in line:
                        coverage_data["meets_minimum_threshold"] = True
                    elif "Does not meet minimum coverage requirements" in line:
                        coverage_data["meets_minimum_threshold"] = False
                    elif "Reports generated:" in line:
                        # 다음 몇 라인에서 리포트 파일들 찾기
                        report_index = lines.index(line)
                        for report_line in lines[report_index+1:report_index+5]:
                            if report_line.strip() and ("Raw data:" in report_line or 
                                                      "Summary:" in report_line or 
                                                      "Markdown:" in report_line or 
                                                      "CSV tracking:" in report_line):
                                coverage_data["reports_generated"].append(report_line.strip())
            
            self.test_results["coverage_analysis"] = coverage_data
            
            if result.returncode == 0:
                print(f"✅ 커버리지 분석 완료 ({time.time() - coverage_start_time:.1f}초)")
                print(f"📊 전체 커버리지: {coverage_data['overall_coverage']:.2f}% (Grade: {coverage_data['grade']})")
                threshold_status = "✅ 통과" if coverage_data["meets_minimum_threshold"] else "❌ 미달"
                print(f"🎯 최소 임계값: {threshold_status}")
            else:
                print(f"❌ 커버리지 분석 실패 ({time.time() - coverage_start_time:.1f}초)")
                if result.stderr:
                    print(f"오류: {result.stderr}")
            
        except subprocess.TimeoutExpired:
            print("❌ 커버리지 분석 시간 초과 (10분)")
            self.test_results["coverage_analysis"] = {
                "status": "timeout",
                "duration": time.time() - coverage_start_time,
                "error": "Analysis timed out after 10 minutes"
            }
        except Exception as e:
            print(f"❌ 커버리지 분석 실패: {e}")
            self.test_results["coverage_analysis"] = {
                "status": "failed",
                "error": str(e),
                "duration": time.time() - coverage_start_time
            }
    
    def run_funnel_integration_tests(self):
        """Funnel 서비스 통합 테스트 실행"""
        print("\n" + "="*80)
        print("🔄 Funnel 서비스 통합 테스트 시작")
        print("="*80)
        
        funnel_start_time = time.time()
        
        try:
            # 프로젝트 루트 디렉토리 찾기
            current_dir = Path(__file__).parent.parent.parent
            
            # Funnel 통합 테스트 파일들
            funnel_test_files = [
                "tests/integration/test_funnel_service_integration.py",
                "tests/integration/test_funnel_google_sheets_integration.py", 
                "tests/integration/test_funnel_schema_generation_integration.py"
            ]
            
            funnel_results = {
                "status": "completed",
                "test_results": [],
                "duration": 0,
                "total_tests": 0,
                "passed_tests": 0,
                "failed_tests": 0
            }
            
            # 각 테스트 파일 실행
            for test_file in funnel_test_files:
                test_file_path = current_dir / test_file
                
                if not test_file_path.exists():
                    print(f"  ⚠️  테스트 파일 없음: {test_file}")
                    continue
                
                print(f"🧪 실행 중: {test_file}")
                
                try:
                    # pytest로 개별 테스트 파일 실행
                    result = subprocess.run([
                        "python", "-m", "pytest",
                        str(test_file_path),
                        "-v",
                        "--tb=short"
                    ],
                    cwd=current_dir,
                    capture_output=True,
                    text=True,
                    timeout=300  # 5분 타임아웃
                    )
                    
                    # 결과 파싱
                    test_result = {
                        "test_file": test_file,
                        "return_code": result.returncode,
                        "success": result.returncode == 0,
                        "stdout": result.stdout,
                        "stderr": result.stderr
                    }
                    
                    # 테스트 결과에서 통계 추출
                    if result.stdout:
                        lines = result.stdout.split('\n')
                        for line in lines:
                            if "passed" in line or "failed" in line:
                                # "=== 5 passed, 2 failed in 30.5s ===" 형식 파싱
                                if "passed" in line:
                                    try:
                                        passed_count = int(line.split()[1]) if "passed" in line.split() else 0
                                        test_result["passed"] = passed_count
                                        funnel_results["passed_tests"] += passed_count
                                        funnel_results["total_tests"] += passed_count
                                    except (ValueError, IndexError):
                                        pass
                                
                                if "failed" in line:
                                    try:
                                        failed_count = int(line.split()[1]) if "failed" in line.split() else 0
                                        test_result["failed"] = failed_count
                                        funnel_results["failed_tests"] += failed_count
                                        funnel_results["total_tests"] += failed_count
                                    except (ValueError, IndexError):
                                        pass
                    
                    funnel_results["test_results"].append(test_result)
                    
                    if result.returncode == 0:
                        print(f"  ✅ {test_file}: 성공")
                    else:
                        print(f"  ❌ {test_file}: 실패 ({result.returncode})")
                        if result.stderr:
                            print(f"      오류: {result.stderr[:200]}...")
                
                except subprocess.TimeoutExpired:
                    print(f"  ⏰ {test_file}: 타임아웃 (5분)")
                    funnel_results["test_results"].append({
                        "test_file": test_file,
                        "success": False,
                        "error": "timeout"
                    })
                except Exception as e:
                    print(f"  ❌ {test_file}: 실행 실패 - {e}")
                    funnel_results["test_results"].append({
                        "test_file": test_file,
                        "success": False,
                        "error": str(e)
                    })
            
            funnel_results["duration"] = time.time() - funnel_start_time
            self.test_results["funnel_integration_tests"] = funnel_results
            
            # 결과 요약
            success_rate = (funnel_results["passed_tests"] / funnel_results["total_tests"]) if funnel_results["total_tests"] > 0 else 0
            print(f"\n📊 Funnel 통합 테스트 결과:")
            print(f"  총 테스트: {funnel_results['total_tests']}")
            print(f"  성공: {funnel_results['passed_tests']}")
            print(f"  실패: {funnel_results['failed_tests']}")
            print(f"  성공률: {success_rate:.1%}")
            print(f"  소요시간: {funnel_results['duration']:.1f}초")
            
            if success_rate >= 0.8:
                print("✅ Funnel 통합 테스트 완료")
            else:
                print("⚠️ Funnel 통합 테스트에서 문제 발견")
            
        except Exception as e:
            print(f"❌ Funnel 통합 테스트 실패: {e}")
            self.test_results["funnel_integration_tests"] = {
                "status": "failed",
                "error": str(e),
                "duration": time.time() - funnel_start_time
            }
    
    def calculate_production_readiness_score(self):
        """프로덕션 준비도 점수 계산"""
        score = 0
        max_score = 100
        
        # 서비스 가용성 (12점)
        service_score = sum(self.services_status.values()) / len(self.services_status) * 12
        score += service_score
        
        # 통합 테스트 결과 (28점)
        integration_tests = self.test_results.get("integration_tests", {})
        if integration_tests.get("status") == "completed":
            success_rate = integration_tests.get("success_rate", 0)
            integration_score = success_rate * 28
            score += integration_score
            
            # 보안 위반 차감
            security_violations = len(integration_tests.get("security_violations", []))
            score -= security_violations * 5  # 위반당 5점 차감
        
        # Funnel 통합 테스트 결과 (15점)
        funnel_tests = self.test_results.get("funnel_integration_tests", {})
        if funnel_tests.get("status") == "completed":
            total_tests = funnel_tests.get("total_tests", 0)
            passed_tests = funnel_tests.get("passed_tests", 0)
            
            if total_tests > 0:
                funnel_success_rate = passed_tests / total_tests
                funnel_score = funnel_success_rate * 15
                score += funnel_score
            
            # Funnel 서비스 중요성으로 인한 가중치
            if self.services_status.get("funnel", False):
                # Funnel 서비스가 실행 중이면 보너스 점수
                score += 2
        
        # 성능 테스트 결과 (25점)
        performance_tests = self.test_results.get("performance_tests", {})
        if performance_tests.get("status") == "completed":
            # 성능 임계값 위반 확인
            performance_violations = 0
            thresholds = performance_tests.get("thresholds", {})
            performance_results = performance_tests.get("performance_results", {})
            
            for test_name, results in performance_results.items():
                if isinstance(results, dict) and "error" not in results:
                    # 응답 시간, 처리량, 에러율 확인
                    if "response_time" in results and results["response_time"] > thresholds.get("max_response_time", 5):
                        performance_violations += 1
                    if "throughput" in results and results["throughput"] < thresholds.get("min_throughput", 10):
                        performance_violations += 1
                    if "error_rate" in results and results["error_rate"] > thresholds.get("max_error_rate", 0.05):
                        performance_violations += 1
            
            performance_score = max(0, 25 - performance_violations * 5)
            score += performance_score
        
        # 테스트 커버리지 분석 (20점)
        coverage_analysis = self.test_results.get("coverage_analysis", {})
        if coverage_analysis.get("status") == "completed":
            overall_coverage = coverage_analysis.get("overall_coverage", 0)
            meets_threshold = coverage_analysis.get("meets_minimum_threshold", False)
            
            # 커버리지 점수 계산
            if overall_coverage >= 95:
                coverage_score = 20  # Excellent
            elif overall_coverage >= 85:
                coverage_score = 18  # Good
            elif overall_coverage >= 75:
                coverage_score = 15  # Acceptable
            elif overall_coverage >= 60:
                coverage_score = 10  # Minimum
            else:
                coverage_score = 5   # Below minimum
            
            # 최소 임계값 미달 시 추가 차감
            if not meets_threshold:
                coverage_score = max(0, coverage_score - 5)
            
            score += coverage_score
        
        return min(max_score, max(0, score))
    
    def generate_final_report(self):
        """최종 종합 보고서 생성"""
        end_time = datetime.now()
        total_duration = (end_time - self.test_start_time).total_seconds()
        
        self.test_results["end_time"] = end_time.isoformat()
        self.test_results["total_duration"] = total_duration
        self.test_results["production_readiness_score"] = self.calculate_production_readiness_score()
        self.test_results["overall_status"] = "completed"
        
        print("\n" + "="*80)
        print("📋 최종 종합 테스트 보고서")
        print("="*80)
        
        print(f"🕒 테스트 시작: {self.test_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🕒 테스트 종료: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️ 총 소요 시간: {total_duration:.1f}초")
        
        print(f"\n🔧 서비스 상태:")
        for service, status in self.services_status.items():
            status_icon = "✅" if status else "❌"
            print(f"  {status_icon} {service.upper()}: {'실행 중' if status else '연결 불가'}")
        
        # 통합 테스트 결과
        integration_tests = self.test_results.get("integration_tests", {})
        if integration_tests.get("status") == "completed":
            test_results = integration_tests.get("test_results", [])
            passed_tests = [t for t in test_results if t["status"] == "passed"]
            failed_tests = [t for t in test_results if t["status"] == "failed"]
            security_violations = integration_tests.get("security_violations", [])
            
            print(f"\n🔗 통합 테스트 결과:")
            print(f"  ✅ 성공: {len(passed_tests)}")
            print(f"  ❌ 실패: {len(failed_tests)}")
            print(f"  🛡️ 보안 위반: {len(security_violations)}")
            print(f"  📊 성공률: {integration_tests.get('success_rate', 0):.1%}")
            
            if failed_tests:
                print(f"  실패한 테스트:")
                for test in failed_tests:
                    print(f"    - {test['test']}: {test.get('error', 'Unknown error')}")
        
        # 성능 테스트 결과
        performance_tests = self.test_results.get("performance_tests", {})
        if performance_tests.get("status") == "completed":
            performance_results = performance_tests.get("performance_results", {})
            resource_metrics = performance_tests.get("resource_metrics", {})
            
            print(f"\n⚡ 성능 테스트 결과:")
            print(f"  📊 테스트 항목: {len(performance_results)}")
            print(f"  💻 리소스 모니터링: {len(resource_metrics)}")
            
            # 주요 성능 지표
            if "response_time_analysis" in performance_results:
                response_analysis = performance_results["response_time_analysis"]
                print(f"  ⏱️ 평균 응답 시간: {response_analysis.get('avg_time', 0):.3f}s")
                print(f"  📈 95%ile 응답 시간: {response_analysis.get('p95_time', 0):.3f}s")
            
            # 동시 사용자 테스트
            concurrent_tests = [k for k in performance_results.keys() if k.startswith("concurrent_")]
            if concurrent_tests:
                max_concurrent = max([performance_results[k].get("concurrent_users", 0) for k in concurrent_tests])
                print(f"  👥 최대 동시 사용자: {max_concurrent}")
        
        # Funnel 통합 테스트 결과
        funnel_tests = self.test_results.get("funnel_integration_tests", {})
        if funnel_tests.get("status") == "completed":
            print(f"\n🔄 Funnel 통합 테스트 결과:")
            print(f"  📊 총 테스트: {funnel_tests.get('total_tests', 0)}")
            print(f"  ✅ 성공: {funnel_tests.get('passed_tests', 0)}")
            print(f"  ❌ 실패: {funnel_tests.get('failed_tests', 0)}")
            
            if funnel_tests.get("total_tests", 0) > 0:
                success_rate = funnel_tests.get("passed_tests", 0) / funnel_tests.get("total_tests", 1)
                print(f"  📈 성공률: {success_rate:.1%}")
            
            print(f"  ⏱️ 소요시간: {funnel_tests.get('duration', 0):.1f}초")
            
            # 개별 테스트 파일 결과
            test_results = funnel_tests.get("test_results", [])
            if test_results:
                failed_tests = [t for t in test_results if not t.get("success", True)]
                if failed_tests:
                    print(f"  실패한 테스트 파일:")
                    for test in failed_tests:
                        test_file = test.get("test_file", "unknown")
                        error = test.get("error", "unknown error")
                        print(f"    - {test_file}: {error}")
        elif funnel_tests.get("status") == "failed":
            print(f"\n🔄 Funnel 통합 테스트 결과:")
            print(f"  ❌ 실행 실패: {funnel_tests.get('error', 'Unknown error')}")
        else:
            print(f"\n🔄 Funnel 통합 테스트:")
            print(f"  ⚠️ 테스트가 실행되지 않았습니다")
        
        # 커버리지 분석 결과
        coverage_analysis = self.test_results.get("coverage_analysis", {})
        if coverage_analysis.get("status") == "completed":
            print(f"\n📊 테스트 커버리지 분석 결과:")
            print(f"  📈 전체 커버리지: {coverage_analysis.get('overall_coverage', 0):.2f}%")
            print(f"  🎯 등급: {coverage_analysis.get('grade', 'N/A')}")
            
            threshold_status = "✅ 통과" if coverage_analysis.get("meets_minimum_threshold", False) else "❌ 미달"
            print(f"  🎲 최소 임계값: {threshold_status}")
            
            reports_generated = coverage_analysis.get("reports_generated", [])
            if reports_generated:
                print(f"  📄 생성된 리포트: {len(reports_generated)}개")
                print("  📄 HTML 리포트: htmlcov/index.html")
        elif coverage_analysis.get("status") == "failed":
            print(f"\n📊 테스트 커버리지 분석 결과:")
            print(f"  ❌ 분석 실패: {coverage_analysis.get('error', 'Unknown error')}")
        elif coverage_analysis.get("status") == "timeout":
            print(f"\n📊 테스트 커버리지 분석 결과:")
            print(f"  ⏰ 분석 시간 초과 (10분)")
        else:
            print(f"\n📊 테스트 커버리지 분석:")
            print(f"  ⚠️ 분석이 실행되지 않았습니다")
        
        # 프로덕션 준비도 점수
        readiness_score = self.test_results["production_readiness_score"]
        print(f"\n🎯 프로덕션 준비도 점수: {readiness_score:.1f}/100")
        
        if readiness_score >= 80:
            print("🚀 프로덕션 배포 준비 완료!")
            recommendation = "시스템이 프로덕션 환경에 배포할 준비가 되었습니다."
        elif readiness_score >= 60:
            print("⚠️ 프로덕션 배포 전 개선 권장")
            recommendation = "일부 성능 및 안정성 개선 후 배포를 권장합니다."
        else:
            print("🚨 프로덕션 배포 부적합")
            recommendation = "심각한 문제들을 해결한 후 재테스트가 필요합니다."
        
        print(f"\n💡 권장 사항: {recommendation}")
        
        # JSON 보고서 저장
        # Save to tests/results directory
        results_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'results')
        os.makedirs(results_dir, exist_ok=True)
        
        report_filename = f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        report_filepath = os.path.join(results_dir, report_filename)
        
        with open(report_filepath, 'w', encoding='utf-8') as f:
            json.dump(self.test_results, f, indent=2, ensure_ascii=False)
        
        print(f"\n📄 상세 보고서 저장: {report_filepath}")
        
        return readiness_score >= 80
    
    async def run_all_tests(self):
        """모든 테스트 실행"""
        print("🚀 SPICE HARVESTER 프로덕션 레벨 종합 테스트 시작")
        print("think ultra: 실제 프로덕션 환경 준비도 검증")
        
        # 1. 전제 조건 확인
        if not await self.check_service_prerequisites():
            print("❌ 전제 조건 확인 실패")
            return False
        
        # 2. 통합 테스트 실행
        await self.run_integration_tests()
        
        # 3. 성능 테스트 실행
        await self.run_performance_tests()
        
        # 4. Funnel 통합 테스트 실행
        self.run_funnel_integration_tests()
        
        # 5. 테스트 커버리지 분석 실행
        self.run_coverage_analysis()
        
        # 6. 최종 보고서 생성
        production_ready = self.generate_final_report()
        
        return production_ready

async def main():
    """메인 실행 함수"""
    print("="*80)
    print("🎯 SPICE HARVESTER 프로덕션 준비도 검증")
    print("="*80)
    
    test_runner = ComprehensiveTestRunner()
    
    try:
        production_ready = await test_runner.run_all_tests()
        
        if production_ready:
            print("\n🎉 모든 테스트 통과! 프로덕션 배포 준비 완료!")
            sys.exit(0)
        else:
            print("\n⚠️ 일부 테스트 실패. 문제 해결 후 재테스트 필요.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⏹️ 사용자에 의해 테스트 중단됨")
        sys.exit(2)
    except Exception as e:
        print(f"\n💥 예상치 못한 오류 발생: {e}")
        sys.exit(3)

if __name__ == "__main__":
    asyncio.run(main())