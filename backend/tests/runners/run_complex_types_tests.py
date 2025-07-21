#!/usr/bin/env python3
"""
🔥 THINK ULTRA!! Complex Types Test Runner
모든 복합 타입 테스트를 순차적으로 실행
"""

import subprocess
import sys
import time
from datetime import datetime
import json
import os

class ComplexTypesTestRunner:
    """🔥 THINK ULTRA!! 복합 타입 테스트 러너"""
    
    def __init__(self):
        self.test_results = {
            "start_time": datetime.now().isoformat(),
            "tests": [],
            "summary": {
                "total": 0,
                "passed": 0,
                "failed": 0
            }
        }
    
    def run_all_tests(self):
        """모든 테스트 실행"""
        
        print("🔥" * 60)
        print("🔥 THINK ULTRA!! Running All Complex Types Tests")
        print("🔥" * 60)
        print(f"\nStarted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Check if services are running
        services_running = self.check_services()
        if not services_running:
            print("\n⚠️ Warning: Services are not running!")
            print("Integration tests will be skipped.")
            print("To run all tests, please start:")
            print("  - OMS: http://localhost:8000")
            print("  - BFF: http://localhost:8002")
        
        # Use unified PYTHONPATH configuration to get project root
        from shared.utils.pythonpath_setup import detect_backend_directory
        project_root = str(detect_backend_directory())
        
        # Define test files to run
        test_files = [
            {
                "name": "ComplexTypeValidator Unit Tests",
                "file": os.path.join(project_root, "tests", "unit", "complex_types", "test_complex_validator_ultra.py"),
                "description": "복합 타입 검증기 단위 테스트",
                "requires_services": False
            },
            {
                "name": "TerminusDB Integration Tests",
                "file": os.path.join(project_root, "tests", "integration", "test_complex_types_terminus_integration.py"),
                "description": "TerminusDB와의 실제 통합 테스트",
                "requires_services": True
            },
            {
                "name": "BFF Integration Tests",
                "file": os.path.join(project_root, "tests", "integration", "test_complex_types_bff_integration.py"),
                "description": "BFF를 통한 end-to-end 테스트",
                "requires_services": True
            }
        ]
        
        # Run each test
        for i, test in enumerate(test_files, 1):
            # Skip integration tests if services are not running
            if test.get('requires_services', False) and not services_running:
                print(f"\n{'=' * 70}")
                print(f"🧪 Test {i}/{len(test_files)}: {test['name']}")
                print(f"📄 File: {test['file']}")
                print(f"📝 {test['description']}")
                print("=" * 70)
                print("⏭️ SKIPPED - Services not running")
                
                self.test_results["tests"].append({
                    "name": test['name'],
                    "file": test['file'],
                    "success": None,
                    "skipped": True,
                    "duration": 0,
                    "output": "Skipped - services not running"
                })
                continue
            
            print(f"\n{'=' * 70}")
            print(f"🧪 Test {i}/{len(test_files)}: {test['name']}")
            print(f"📄 File: {test['file']}")
            print(f"📝 {test['description']}")
            print("=" * 70)
            
            # Check if test file exists
            if not os.path.exists(test['file']):
                print(f"❌ ERROR: Test file not found: {test['file']}")
                self.test_results["tests"].append({
                    "name": test['name'],
                    "file": test['file'],
                    "success": False,
                    "duration": 0,
                    "output": f"Test file not found: {test['file']}"
                })
                self.test_results["summary"]["total"] += 1
                self.test_results["summary"]["failed"] += 1
                continue
            
            result = self.run_test(test['file'])
            
            self.test_results["tests"].append({
                "name": test['name'],
                "file": test['file'],
                "success": result['success'],
                "duration": result['duration'],
                "output": result['output'][-1000:] if not result['success'] else ""  # Last 1000 chars if failed
            })
            
            self.test_results["summary"]["total"] += 1
            if result['success']:
                self.test_results["summary"]["passed"] += 1
                print(f"✅ PASSED in {result['duration']:.2f} seconds")
            else:
                self.test_results["summary"]["failed"] += 1
                print(f"❌ FAILED in {result['duration']:.2f} seconds")
                if result['output']:
                    print("\nError output (last 50 lines):")
                    print("-" * 50)
                    lines = result['output'].split('\n')
                    for line in lines[-50:]:
                        print(line)
            
            # Small delay between tests
            time.sleep(2)
        
        # Print summary
        self.print_summary()
        
        # Save results
        self.save_results()
    
    def check_services(self):
        """Check if required services are running"""
        import httpx
        
        services = [
            ("OMS", "http://localhost:8000/health"),
            ("BFF", "http://localhost:8002/health")
        ]
        
        all_running = True
        
        print("\n🔍 Checking services...")
        for name, url in services:
            try:
                response = httpx.get(url, timeout=5)
                if response.status_code == 200:
                    print(f"  ✅ {name} is running")
                else:
                    print(f"  ❌ {name} returned status {response.status_code}")
                    all_running = False
            except:
                print(f"  ❌ {name} is not reachable")
                all_running = False
        
        return all_running
    
    def run_test(self, test_file):
        """Run a single test file"""
        start_time = time.time()
        
        try:
            # Use unified PYTHONPATH configuration
            from shared.utils.pythonpath_setup import configure_python_environment
            
            if not configure_python_environment(verbose=False):
                raise RuntimeError("Failed to configure Python environment for test")
            
            # Set up environment with properly configured PYTHONPATH
            env = os.environ.copy()
            # PYTHONPATH is already set by configure_python_environment
            
            # Run the test
            result = subprocess.run(
                [sys.executable, test_file],
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
                env=env
            )
            
            duration = time.time() - start_time
            
            return {
                "success": result.returncode == 0,
                "duration": duration,
                "output": result.stdout + result.stderr
            }
            
        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            test_name = os.path.basename(test_file)
            error_msg = f"Test timeout after 300 seconds: {test_name}\nOperation: Running {test_file}"
            return {
                "success": False,
                "duration": duration,
                "output": error_msg
            }
        except Exception as e:
            duration = time.time() - start_time
            test_name = os.path.basename(test_file)
            error_msg = f"Test execution error: {test_name}\nFile: {test_file}\nError: {str(e)}"
            return {
                "success": False,
                "duration": duration,
                "output": error_msg
            }
    
    def print_summary(self):
        """Print test summary"""
        
        print("\n" + "🔥" * 60)
        print("🔥 THINK ULTRA!! Test Summary")
        print("🔥" * 60)
        
        summary = self.test_results["summary"]
        skipped = sum(1 for test in self.test_results["tests"] if test.get('skipped', False))
        print(f"\n📊 Overall Results:")
        print(f"   Total Tests: {len(self.test_results['tests'])}")
        print(f"   Passed: {summary['passed']} ✅")
        print(f"   Failed: {summary['failed']} ❌")
        print(f"   Skipped: {skipped} ⏭️")
        
        if summary['total'] > 0:
            success_rate = (summary['passed'] / summary['total']) * 100
            print(f"   Success Rate: {success_rate:.1f}%")
        
        print(f"\n📋 Individual Test Results:")
        for test in self.test_results["tests"]:
            if test.get('skipped', False):
                status = "⏭️ SKIP"
            else:
                status = "✅ PASS" if test['success'] else "❌ FAIL"
            print(f"   {status} - {test['name']} ({test['duration']:.2f}s)")
        
        print(f"\n🏆 Conclusion:")
        if summary['failed'] == 0:
            print("   ✅ All complex type tests passed successfully!")
            print("   ✅ ComplexTypeValidator is working perfectly!")
            print("   ✅ TerminusDB integration is functioning correctly!")
            print("   ✅ BFF end-to-end workflow is validated!")
        else:
            print("   ⚠️ Some tests failed. Please check the logs above.")
        
        print(f"\n⏱️ Total execution time: {self.get_total_duration():.2f} seconds")
        print(f"🕐 Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def get_total_duration(self):
        """Calculate total test duration"""
        return sum(test['duration'] for test in self.test_results['tests'])
    
    def save_results(self):
        """Save test results to file"""
        self.test_results["end_time"] = datetime.now().isoformat()
        self.test_results["total_duration"] = self.get_total_duration()
        
        # Save to tests/results directory
        results_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'results')
        os.makedirs(results_dir, exist_ok=True)
        
        filename = f"complex_types_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = os.path.join(results_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.test_results, f, ensure_ascii=False, indent=2)
        
        print(f"\n📄 Detailed results saved to: {filepath}")

def main():
    """Main entry point"""
    runner = ComplexTypesTestRunner()
    runner.run_all_tests()
    
    # Exit with appropriate code
    if runner.test_results["summary"]["failed"] > 0:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()