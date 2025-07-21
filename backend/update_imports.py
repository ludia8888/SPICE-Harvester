#!/usr/bin/env python3
"""
🔥 THINK ULTRA!! Import Update Script
Updates all imports from 'spice_harvester.*' to direct module imports
"""

import os
import re
from pathlib import Path

def update_imports_in_file(file_path):
    """Update imports in a single file"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    
    # Pattern replacements
    replacements = [
        # from anything import -> from anything import
        (r'from spice_harvester\.([^\s]+)', r'from \1'),
        # import anything -> import anything
        (r'import spice_harvester\.([^\s]+)', r'import \1'),
    ]
    
    for pattern, replacement in replacements:
        content = re.sub(pattern, replacement, content)
    
    if content != original_content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False

def find_python_files(root_dir):
    """Find all Python files excluding backup directories"""
    python_files = []
    for root, dirs, files in os.walk(root_dir):
        # Skip backup directories
        if 'backup' in root:
            continue
        for file in files:
            if file.endswith('.py'):
                python_files.append(os.path.join(root, file))
    return python_files

def main():
    root_dir = "/Users/isihyeon/Desktop/SPICE HARVESTER/backend"
    
    print("🔥 THINK ULTRA!! Updating imports...")
    print(f"Root directory: {root_dir}")
    
    python_files = find_python_files(root_dir)
    print(f"Found {len(python_files)} Python files")
    
    updated_count = 0
    for file_path in python_files:
        if update_imports_in_file(file_path):
            print(f"✅ Updated: {file_path}")
            updated_count += 1
    
    print(f"\n🔥 Updated {updated_count} files")
    print("Import update complete!")

if __name__ == "__main__":
    main()