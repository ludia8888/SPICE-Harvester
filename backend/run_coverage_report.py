#!/usr/bin/env python3
"""
🔥 THINK ULTRA! Comprehensive Test Coverage Reporter
Enhanced test coverage measurement and reporting with detailed analysis
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

import xml.etree.ElementTree as ET


class CoverageReporter:
    """🔥 THINK ULTRA! Comprehensive coverage reporter with enhanced analysis"""
    
    def __init__(self, project_root: Optional[str] = None):
        self.project_root = Path(project_root) if project_root else Path(__file__).parent
        self.coverage_dir = self.project_root / "htmlcov"
        self.reports_dir = self.project_root / "tests" / "results" / "coverage"
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        
        # Coverage thresholds
        self.thresholds = {
            "excellent": 95.0,
            "good": 85.0,
            "acceptable": 75.0,
            "minimum": 60.0
        }
        
        # Module priority for coverage analysis
        self.module_priorities = {
            "shared": "high",     # Core shared functionality
            "bff": "high",        # Backend for frontend
            "oms": "high",        # Ontology management service
            "funnel": "medium",   # Data processing funnel
            "data_connector": "medium"  # Data connectors
        }
    
    def run_coverage_analysis(self, test_pattern: str = "tests/", include_integration: bool = True, 
                            include_performance: bool = False) -> Dict[str, Any]:
        """Run comprehensive coverage analysis"""
        print("🔥 THINK ULTRA! Running comprehensive coverage analysis...")
        print("=" * 80)
        
        analysis_start = time.time()
        
        # Prepare coverage command
        coverage_args = [
            "pytest",
            "--cov=.",
            "--cov-branch",
            "--cov-report=term-missing:skip-covered",
            "--cov-report=html",
            "--cov-report=xml",
            "-v"
        ]
        
        # Add test patterns
        if test_pattern:
            coverage_args.append(test_pattern)
        
        # Filter tests based on options
        test_markers = []
        if not include_integration:
            test_markers.append("not integration")
        if not include_performance:
            test_markers.append("not performance")
        
        if test_markers:
            coverage_args.extend(["-m", " and ".join(test_markers)])
        
        print(f"📊 Running coverage with: {' '.join(coverage_args)}")
        print()
        
        # Run coverage
        try:
            result = subprocess.run(
                coverage_args,
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=600  # 10 minute timeout
            )
            
            analysis_time = time.time() - analysis_start
            
            # Parse results
            coverage_data = self._parse_coverage_results(result, analysis_time)
            
            # Generate enhanced reports
            self._generate_detailed_reports(coverage_data)
            
            # Print summary
            self._print_coverage_summary(coverage_data)
            
            return coverage_data
            
        except subprocess.TimeoutExpired:
            print("❌ Coverage analysis timed out after 10 minutes")
            return {"error": "timeout", "analysis_time": 600}
        except Exception as e:
            print(f"❌ Coverage analysis failed: {e}")
            return {"error": str(e), "analysis_time": time.time() - analysis_start}
    
    def _parse_coverage_results(self, result: subprocess.CompletedProcess, analysis_time: float) -> Dict[str, Any]:
        """Parse coverage results from subprocess output"""
        coverage_data = {
            "timestamp": datetime.now().isoformat(),
            "analysis_time": analysis_time,
            "return_code": result.returncode,
            "success": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "overall_coverage": 0.0,
            "module_coverage": {},
            "file_coverage": {},
            "missing_lines": {},
            "branch_coverage": 0.0,
            "test_results": {}
        }
        
        # Parse overall coverage from stdout
        stdout_lines = result.stdout.split('\n')
        for line in stdout_lines:
            if "TOTAL" in line and "%" in line:
                # Extract overall coverage percentage
                parts = line.split()
                for part in parts:
                    if part.endswith('%'):
                        try:
                            coverage_data["overall_coverage"] = float(part.rstrip('%'))
                            break
                        except ValueError:
                            continue
        
        # Parse XML coverage report for detailed data
        xml_path = self.project_root / "coverage.xml"
        if xml_path.exists():
            coverage_data.update(self._parse_xml_coverage(xml_path))
        
        # Parse test results from stdout
        coverage_data["test_results"] = self._parse_test_results(result.stdout)
        
        return coverage_data
    
    def _parse_xml_coverage(self, xml_path: Path) -> Dict[str, Any]:
        """Parse detailed coverage data from XML report"""
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            xml_data = {
                "module_coverage": {},
                "file_coverage": {},
                "missing_lines": {},
                "branch_coverage": 0.0
            }
            
            # Parse packages (modules)
            for package in root.findall(".//package"):
                package_name = package.get("name", "unknown")
                line_rate = float(package.get("line-rate", 0)) * 100
                branch_rate = float(package.get("branch-rate", 0)) * 100
                
                xml_data["module_coverage"][package_name] = {
                    "line_coverage": line_rate,
                    "branch_coverage": branch_rate,
                    "priority": self.module_priorities.get(package_name, "low")
                }
            
            # Parse individual files
            for class_elem in root.findall(".//class"):
                filename = class_elem.get("filename", "unknown")
                line_rate = float(class_elem.get("line-rate", 0)) * 100
                branch_rate = float(class_elem.get("branch-rate", 0)) * 100
                
                xml_data["file_coverage"][filename] = {
                    "line_coverage": line_rate,
                    "branch_coverage": branch_rate
                }
                
                # Parse missing lines
                missing_lines = []
                for line in class_elem.findall(".//line"):
                    if line.get("hits") == "0":
                        missing_lines.append(int(line.get("number", 0)))
                
                if missing_lines:
                    xml_data["missing_lines"][filename] = missing_lines
            
            # Calculate overall branch coverage
            total_branches = 0
            covered_branches = 0
            for package in root.findall(".//package"):
                branches = int(package.get("branches", 0))
                branch_rate = float(package.get("branch-rate", 0))
                total_branches += branches
                covered_branches += int(branches * branch_rate)
            
            if total_branches > 0:
                xml_data["branch_coverage"] = (covered_branches / total_branches) * 100
            
            return xml_data
            
        except Exception as e:
            print(f"⚠️  Could not parse XML coverage: {e}")
            return {}
    
    def _parse_test_results(self, stdout: str) -> Dict[str, Any]:
        """Parse test execution results"""
        test_data = {
            "total_tests": 0,
            "passed": 0,
            "failed": 0,
            "skipped": 0,
            "errors": 0,
            "warnings": 0,
            "duration": 0.0
        }
        
        lines = stdout.split('\n')
        
        for line in lines:
            # Parse test summary line like "=== 42 passed, 2 failed, 1 skipped in 15.23s ==="
            if "passed" in line and ("failed" in line or "skipped" in line or "error" in line):
                parts = line.split()
                for i, part in enumerate(parts):
                    if part == "passed" and i > 0:
                        test_data["passed"] = int(parts[i-1])
                        test_data["total_tests"] += test_data["passed"]
                    elif part == "failed" and i > 0:
                        test_data["failed"] = int(parts[i-1])
                        test_data["total_tests"] += test_data["failed"]
                    elif part == "skipped" and i > 0:
                        test_data["skipped"] = int(parts[i-1])
                        test_data["total_tests"] += test_data["skipped"]
                    elif part == "error" and i > 0:
                        test_data["errors"] = int(parts[i-1])
                        test_data["total_tests"] += test_data["errors"]
                    elif part.endswith("s") and "." in part:
                        try:
                            test_data["duration"] = float(part.rstrip('s'))
                        except ValueError:
                            pass
        
        return test_data
    
    def _generate_detailed_reports(self, coverage_data: Dict[str, Any]):
        """Generate detailed coverage reports"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save raw coverage data
        raw_report_path = self.reports_dir / f"coverage_raw_{timestamp}.json"
        with open(raw_report_path, 'w') as f:
            json.dump(coverage_data, f, indent=2, default=str)
        
        # Generate summary report
        summary_report = self._generate_summary_report(coverage_data)
        summary_path = self.reports_dir / f"coverage_summary_{timestamp}.json"
        with open(summary_path, 'w') as f:
            json.dump(summary_report, f, indent=2)
        
        # Generate markdown report
        markdown_report = self._generate_markdown_report(coverage_data)
        markdown_path = self.reports_dir / f"coverage_report_{timestamp}.md"
        with open(markdown_path, 'w') as f:
            f.write(markdown_report)
        
        # Generate CSV report for tracking
        csv_report = self._generate_csv_report(coverage_data)
        csv_path = self.reports_dir / f"coverage_tracking_{timestamp}.csv"
        with open(csv_path, 'w') as f:
            f.write(csv_report)
        
        print(f"📄 Reports generated:")
        print(f"  Raw data: {raw_report_path}")
        print(f"  Summary: {summary_path}")
        print(f"  Markdown: {markdown_path}")
        print(f"  CSV tracking: {csv_path}")
    
    def _generate_summary_report(self, coverage_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate summary coverage report"""
        overall_coverage = coverage_data.get("overall_coverage", 0)
        
        # Determine coverage grade
        if overall_coverage >= self.thresholds["excellent"]:
            grade = "A+"
            status = "excellent"
        elif overall_coverage >= self.thresholds["good"]:
            grade = "A"
            status = "good"
        elif overall_coverage >= self.thresholds["acceptable"]:
            grade = "B"
            status = "acceptable"
        elif overall_coverage >= self.thresholds["minimum"]:
            grade = "C"
            status = "needs_improvement"
        else:
            grade = "F"
            status = "critical"
        
        # Analyze module coverage
        module_analysis = {}
        for module, data in coverage_data.get("module_coverage", {}).items():
            line_cov = data.get("line_coverage", 0)
            branch_cov = data.get("branch_coverage", 0)
            priority = data.get("priority", "low")
            
            module_status = "good"
            if line_cov < self.thresholds["minimum"]:
                module_status = "critical"
            elif line_cov < self.thresholds["acceptable"]:
                module_status = "needs_improvement"
            
            module_analysis[module] = {
                "line_coverage": line_cov,
                "branch_coverage": branch_cov,
                "priority": priority,
                "status": module_status,
                "meets_threshold": line_cov >= self.thresholds["minimum"]
            }
        
        # Test execution summary
        test_results = coverage_data.get("test_results", {})
        test_success_rate = 0
        if test_results.get("total_tests", 0) > 0:
            test_success_rate = (test_results.get("passed", 0) / test_results.get("total_tests", 1)) * 100
        
        return {
            "timestamp": coverage_data.get("timestamp"),
            "overall": {
                "line_coverage": overall_coverage,
                "branch_coverage": coverage_data.get("branch_coverage", 0),
                "grade": grade,
                "status": status,
                "meets_minimum": overall_coverage >= self.thresholds["minimum"]
            },
            "modules": module_analysis,
            "test_execution": {
                "total_tests": test_results.get("total_tests", 0),
                "success_rate": test_success_rate,
                "duration": test_results.get("duration", 0),
                "passed": test_results.get("passed", 0),
                "failed": test_results.get("failed", 0),
                "skipped": test_results.get("skipped", 0)
            },
            "thresholds": self.thresholds,
            "analysis_time": coverage_data.get("analysis_time", 0)
        }
    
    def _generate_markdown_report(self, coverage_data: Dict[str, Any]) -> str:
        """Generate markdown coverage report"""
        summary = self._generate_summary_report(coverage_data)
        
        report = f"""# 🔥 THINK ULTRA! Test Coverage Report

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**Analysis Time:** {coverage_data.get('analysis_time', 0):.2f} seconds

## 📊 Overall Coverage

| Metric | Value | Grade | Status |
|--------|-------|-------|--------|
| Line Coverage | {summary['overall']['line_coverage']:.2f}% | {summary['overall']['grade']} | {summary['overall']['status']} |
| Branch Coverage | {summary['overall']['branch_coverage']:.2f}% | - | - |

## 📋 Module Coverage Analysis

| Module | Line Coverage | Branch Coverage | Priority | Status |
|--------|---------------|-----------------|----------|---------|
"""
        
        for module, data in summary['modules'].items():
            status_icon = "✅" if data['meets_threshold'] else "❌"
            report += f"| {module} | {data['line_coverage']:.2f}% | {data['branch_coverage']:.2f}% | {data['priority']} | {status_icon} {data['status']} |\n"
        
        report += f"""
## 🧪 Test Execution Summary

| Metric | Value |
|--------|-------|
| Total Tests | {summary['test_execution']['total_tests']} |
| Success Rate | {summary['test_execution']['success_rate']:.2f}% |
| Passed | {summary['test_execution']['passed']} |
| Failed | {summary['test_execution']['failed']} |
| Skipped | {summary['test_execution']['skipped']} |
| Duration | {summary['test_execution']['duration']:.2f}s |

## 🎯 Coverage Thresholds

| Level | Threshold |
|-------|-----------|
| Excellent | ≥ {self.thresholds['excellent']}% |
| Good | ≥ {self.thresholds['good']}% |
| Acceptable | ≥ {self.thresholds['acceptable']}% |
| Minimum | ≥ {self.thresholds['minimum']}% |

## 📈 Recommendations

"""
        
        # Add recommendations based on coverage analysis
        if summary['overall']['line_coverage'] < self.thresholds['minimum']:
            report += "🚨 **CRITICAL**: Overall coverage is below minimum threshold. Immediate action required.\n\n"
        elif summary['overall']['line_coverage'] < self.thresholds['acceptable']:
            report += "⚠️ **WARNING**: Coverage needs improvement to meet acceptable standards.\n\n"
        
        # Module-specific recommendations
        for module, data in summary['modules'].items():
            if data['priority'] == 'high' and not data['meets_threshold']:
                report += f"- **{module}**: High-priority module with insufficient coverage ({data['line_coverage']:.1f}%). Focus testing efforts here.\n"
        
        report += f"""
## 📄 Generated Reports

- **HTML Report**: `htmlcov/index.html`
- **XML Report**: `coverage.xml`
- **Raw Data**: Available in `tests/results/coverage/`

---
*Generated by SPICE HARVESTER Coverage Reporter*
"""
        
        return report
    
    def _generate_csv_report(self, coverage_data: Dict[str, Any]) -> str:
        """Generate CSV report for coverage tracking"""
        summary = self._generate_summary_report(coverage_data)
        
        csv_lines = [
            "timestamp,overall_line_coverage,overall_branch_coverage,total_tests,passed_tests,failed_tests,test_duration,analysis_time"
        ]
        
        csv_lines.append(
            f"{coverage_data.get('timestamp', '')},"
            f"{summary['overall']['line_coverage']:.2f},"
            f"{summary['overall']['branch_coverage']:.2f},"
            f"{summary['test_execution']['total_tests']},"
            f"{summary['test_execution']['passed']},"
            f"{summary['test_execution']['failed']},"
            f"{summary['test_execution']['duration']:.2f},"
            f"{coverage_data.get('analysis_time', 0):.2f}"
        )
        
        return '\n'.join(csv_lines)
    
    def _print_coverage_summary(self, coverage_data: Dict[str, Any]):
        """Print coverage summary to console"""
        summary = self._generate_summary_report(coverage_data)
        
        print("\n" + "=" * 80)
        print("📊 COVERAGE ANALYSIS COMPLETE")
        print("=" * 80)
        
        overall = summary['overall']
        print(f"🎯 Overall Coverage: {overall['line_coverage']:.2f}% (Grade: {overall['grade']}) - {overall['status']}")
        print(f"🌳 Branch Coverage: {overall['branch_coverage']:.2f}%")
        
        if overall['meets_minimum']:
            print("✅ Meets minimum coverage requirements")
        else:
            print("❌ Does not meet minimum coverage requirements")
        
        print(f"\n🧪 Test Results: {summary['test_execution']['passed']}/{summary['test_execution']['total_tests']} passed")
        print(f"⏱️  Analysis completed in {coverage_data.get('analysis_time', 0):.2f} seconds")
        
        # Show critical modules
        critical_modules = [
            module for module, data in summary['modules'].items()
            if data['priority'] == 'high' and not data['meets_threshold']
        ]
        
        if critical_modules:
            print(f"\n⚠️  Critical modules needing attention: {', '.join(critical_modules)}")
        
        print(f"\n📄 Detailed reports available in: {self.reports_dir}")
        print("📄 HTML report available at: htmlcov/index.html")


def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(
        description="🔥 THINK ULTRA! Comprehensive test coverage analysis"
    )
    
    parser.add_argument(
        "--test-pattern", 
        default="tests/",
        help="Test pattern to run (default: tests/)"
    )
    
    parser.add_argument(
        "--no-integration",
        action="store_true",
        help="Exclude integration tests"
    )
    
    parser.add_argument(
        "--include-performance",
        action="store_true", 
        help="Include performance tests"
    )
    
    parser.add_argument(
        "--project-root",
        help="Project root directory (default: current directory)"
    )
    
    args = parser.parse_args()
    
    # Initialize reporter
    reporter = CoverageReporter(args.project_root)
    
    # Run coverage analysis
    results = reporter.run_coverage_analysis(
        test_pattern=args.test_pattern,
        include_integration=not args.no_integration,
        include_performance=args.include_performance
    )
    
    # Exit with appropriate code
    if results.get("success", False):
        # Check if coverage meets minimum threshold
        overall_coverage = results.get("overall_coverage", 0)
        if overall_coverage >= reporter.thresholds["minimum"]:
            sys.exit(0)
        else:
            print(f"\n❌ Coverage {overall_coverage:.2f}% below minimum {reporter.thresholds['minimum']}%")
            sys.exit(1)
    else:
        print(f"\n❌ Coverage analysis failed")
        sys.exit(1)


if __name__ == "__main__":
    main()