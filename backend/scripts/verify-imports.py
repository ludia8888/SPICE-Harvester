#!/usr/bin/env python3
"""
Import Verification Script

This script verifies that all Python imports can be resolved at build time,
preventing runtime import errors from conditional imports.

Usage:
    python verify-imports.py <service_directory>
    
Example:
    python verify-imports.py bff
    python verify-imports.py oms
"""

import ast
import sys
import os
from pathlib import Path
from typing import Set, List, Tuple
import importlib.util


class ImportChecker(ast.NodeVisitor):
    """AST visitor to extract all import statements."""
    
    def __init__(self):
        self.imports: Set[str] = set()
        self.from_imports: Set[Tuple[str, str]] = set()
        
    def visit_Import(self, node: ast.Import) -> None:
        """Visit import statements."""
        for alias in node.names:
            self.imports.add(alias.name)
        self.generic_visit(node)
        
    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Visit from ... import statements."""
        if node.module:
            for alias in node.names:
                self.from_imports.add((node.module, alias.name))
        self.generic_visit(node)


def extract_imports(file_path: Path) -> Tuple[Set[str], Set[Tuple[str, str]]]:
    """Extract all imports from a Python file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            tree = ast.parse(f.read())
            
        checker = ImportChecker()
        checker.visit(tree)
        return checker.imports, checker.from_imports
        
    except Exception as e:
        print(f"Error parsing {file_path}: {e}")
        return set(), set()


def verify_import(module_name: str) -> bool:
    """Verify that a module can be imported."""
    try:
        spec = importlib.util.find_spec(module_name)
        return spec is not None
    except (ImportError, ModuleNotFoundError, ValueError):
        return False


def check_conditional_imports(file_path: Path) -> List[str]:
    """Check for conditional imports (try/except ImportError patterns)."""
    issues = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Simple pattern matching for try/except ImportError
        if 'except ImportError' in content:
            # More detailed check
            lines = content.split('\n')
            for i, line in enumerate(lines):
                if 'except ImportError' in line:
                    # Look for the try block
                    try_line = None
                    for j in range(max(0, i-10), i):
                        if 'try:' in lines[j]:
                            try_line = j
                            break
                    
                    if try_line is not None:
                        import_lines = []
                        for k in range(try_line + 1, i):
                            if 'import' in lines[k]:
                                import_lines.append(lines[k].strip())
                        
                        if import_lines:
                            issues.append(
                                f"Conditional import found at line {i+1}:\n" +
                                f"  Try block starts at line {try_line+1}\n" +
                                f"  Imports: {', '.join(import_lines)}"
                            )
                            
    except Exception as e:
        print(f"Error checking {file_path}: {e}")
        
    return issues


def verify_service(service_dir: Path) -> Tuple[List[str], List[str]]:
    """Verify all imports in a service directory."""
    import_errors = []
    conditional_imports = []
    
    # Ensure proper Python path setup for relative imports
    backend_dir = service_dir.parent
    if str(backend_dir) not in sys.path:
        sys.path.insert(0, str(backend_dir))
    if str(service_dir) not in sys.path:
        sys.path.insert(0, str(service_dir))
    
    # Find all Python files
    python_files = list(service_dir.rglob("*.py"))
    
    print(f"\nChecking {len(python_files)} Python files in {service_dir.name}...")
    
    for file_path in python_files:
        # Skip test files and __pycache__
        if '__pycache__' in str(file_path) or 'test_' in file_path.name:
            continue
            
        # Check for conditional imports
        issues = check_conditional_imports(file_path)
        if issues:
            for issue in issues:
                conditional_imports.append(f"{file_path}:\n{issue}")
        
        # Extract and verify imports
        imports, from_imports = extract_imports(file_path)
        
        # Check regular imports
        for module in imports:
            if not verify_import(module):
                import_errors.append(
                    f"{file_path}: Cannot import '{module}'"
                )
        
        # Check from imports
        for module, name in from_imports:
            if not verify_import(module):
                import_errors.append(
                    f"{file_path}: Cannot import from '{module}'"
                )
    
    return import_errors, conditional_imports


def main():
    """Main entry point."""
    if len(sys.argv) != 2:
        print("Usage: python verify-imports.py <service_directory>")
        sys.exit(1)
        
    service_name = sys.argv[1]
    backend_dir = Path(__file__).parent.parent
    service_dir = backend_dir / service_name
    
    if not service_dir.exists():
        print(f"Error: Service directory '{service_dir}' does not exist")
        sys.exit(1)
        
    print(f"Verifying imports for service: {service_name}")
    print(f"Service directory: {service_dir}")
    
    # Set PYTHONPATH to include backend directory
    os.environ['PYTHONPATH'] = str(backend_dir)
    sys.path.insert(0, str(backend_dir))
    
    # Verify imports
    import_errors, conditional_imports = verify_service(service_dir)
    
    # Report results
    print("\n" + "="*60)
    print("VERIFICATION RESULTS")
    print("="*60)
    
    if conditional_imports:
        print(f"\n❌ CONDITIONAL IMPORTS FOUND ({len(conditional_imports)}):")
        print("-"*60)
        for issue in conditional_imports:
            print(f"\n{issue}")
    else:
        print("\n✅ No conditional imports found")
    
    if import_errors:
        print(f"\n❌ IMPORT ERRORS FOUND ({len(import_errors)}):")
        print("-"*60)
        for error in import_errors:
            print(f"  {error}")
    else:
        print("\n✅ All imports verified successfully")
    
    print("\n" + "="*60)
    
    # Exit with error code if issues found
    if import_errors or conditional_imports:
        print(f"\n🚨 Total issues: {len(import_errors) + len(conditional_imports)}")
        sys.exit(1)
    else:
        print("\n✅ Build-time import verification passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()