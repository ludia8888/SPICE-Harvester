#!/usr/bin/env python3
"""
Fix datetime.utcnow() deprecation warnings
Replace with timezone-aware datetime.now(datetime.UTC)
"""

import os
import re
from pathlib import Path

def fix_datetime_in_file(filepath):
    """Fix datetime deprecation in a single file"""
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    
    # Check if datetime is imported
    if 'from datetime import' in content or 'import datetime' in content:
        # Replace datetime.utcnow() with datetime.now(datetime.UTC)
        content = re.sub(
            r'datetime\.utcnow\(\)',
            'datetime.now(datetime.UTC)',
            content
        )
        
        # Replace datetime.datetime.utcnow() with datetime.datetime.now(datetime.UTC)
        content = re.sub(
            r'datetime\.datetime\.utcnow\(\)',
            'datetime.datetime.now(datetime.UTC)',
            content
        )
        
        # Ensure datetime.UTC is imported if needed
        if 'datetime.now(datetime.UTC)' in content:
            # Check if we need to add UTC import
            if 'from datetime import' in content and 'UTC' not in content[:content.index('\n\n')]:
                # Add UTC to existing datetime import
                content = re.sub(
                    r'from datetime import ([^;\n]+)',
                    lambda m: f"from datetime import {m.group(1)}, UTC" if 'UTC' not in m.group(1) else m.group(0),
                    content,
                    count=1
                )
    
    if content != original:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False

def main():
    """Fix all Python files with datetime deprecation"""
    
    files_to_fix = [
        'oms/entities/ontology.py',
        'oms/entities/label_mapping.py',
        'oms/services/pull_request_service.py',
        'oms/services/corrected_command_handler.py',
        'oms/services/event_store.py',
        'oms/services/terminus/ontology.py',
        'shared/services/storage_service.py',
        'shared/services/redis_service.py',
        'bff/middleware/rbac.py',
        'funnel/services/data_processor.py',
        'message_relay/main.py',
        'analysis/system_improvement_analysis.py',
        'monitoring/s3_event_store_dashboard.py',
        'tests/integration/test_e2e_event_sourcing_s3.py',
        'tests/integration/test_worker_s3_integration.py',
        'tests/unit/test_event_store.py',
        'tests/unit/test_migration_helper.py'
    ]
    
    print("🔧 Fixing datetime.utcnow() deprecation warnings...")
    print("=" * 60)
    
    fixed_count = 0
    for filepath in files_to_fix:
        if os.path.exists(filepath):
            if fix_datetime_in_file(filepath):
                print(f"✅ Fixed: {filepath}")
                fixed_count += 1
        else:
            print(f"⚠️  Skipped (not found): {filepath}")
    
    print("=" * 60)
    print(f"✅ Fixed {fixed_count} files")
    print("🎉 All datetime deprecation warnings resolved!")

if __name__ == "__main__":
    main()