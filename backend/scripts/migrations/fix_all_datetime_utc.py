#!/usr/bin/env python
"""
Fix all datetime.UTC usage to timezone.utc
Python doesn't have datetime.UTC, it should be timezone.utc
"""

import os
import re

def fix_file(filepath):
    """Fix datetime.UTC to timezone.utc in a file"""
    with open(filepath, 'r') as f:
        content = f.read()
    
    original = content
    changes = []
    
    # Fix imports - if importing UTC from datetime, change to timezone
    if 'from datetime import' in content and 'UTC' in content:
        # Replace "from datetime import ... UTC ..." with timezone
        content = re.sub(r'from datetime import (.*?)UTC', r'from datetime import \1timezone', content)
        changes.append("Fixed import: UTC -> timezone")
    
    # Fix usage of datetime.UTC to timezone.utc
    if 'datetime.UTC' in content:
        content = content.replace('datetime.UTC', 'timezone.utc')
        changes.append("Fixed usage: datetime.UTC -> timezone.utc")
    
    # Fix usage of just UTC (after fixing imports)
    # But be careful not to replace UTC in strings or comments
    content = re.sub(r'\b(?<!datetime\.)(?<!\.)\bUTC\b(?![\'\"])', 'timezone.utc', content)
    if 'timezone.utc' in content and 'timezone.utc' not in original:
        changes.append("Fixed usage: UTC -> timezone.utc")
    
    if content != original:
        with open(filepath, 'w') as f:
            f.write(content)
        return filepath, changes
    return None, []

def main():
    """Fix all Python files in OMS directory"""
    fixed_files = []
    
    # Target directories
    dirs_to_fix = [
        '/Users/isihyeon/Desktop/SPICE HARVESTER/backend/oms',
        '/Users/isihyeon/Desktop/SPICE HARVESTER/backend/shared',
        '/Users/isihyeon/Desktop/SPICE HARVESTER/backend/bff',
    ]
    
    for target_dir in dirs_to_fix:
        for root, dirs, files in os.walk(target_dir):
            # Skip __pycache__ directories
            dirs[:] = [d for d in dirs if d != '__pycache__']
            
            for file in files:
                if file.endswith('.py'):
                    filepath = os.path.join(root, file)
                    fixed, changes = fix_file(filepath)
                    if fixed:
                        fixed_files.append((fixed, changes))
    
    if fixed_files:
        print("🔧 Fixed datetime.UTC issues in the following files:")
        for filepath, changes in fixed_files:
            print(f"\n  ✅ {filepath}")
            for change in changes:
                print(f"     - {change}")
        print(f"\n📊 Total files fixed: {len(fixed_files)}")
    else:
        print("✨ No datetime.UTC issues found!")

if __name__ == "__main__":
    main()