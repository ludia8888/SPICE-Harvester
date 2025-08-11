#!/usr/bin/env python3
"""
Debug Pydantic Settings Loading
Pydantic이 환경변수를 올바르게 로드하는지 테스트
"""

from dotenv import load_dotenv
import os

# Load environment variables first
load_dotenv()

def debug_pydantic():
    print("🔍 Pydantic Settings Loading Debug")
    print("=" * 50)
    
    # Check raw environment variable
    terminus_key = os.getenv("TERMINUS_KEY")
    print(f"📋 Raw TERMINUS_KEY environment variable: '{terminus_key}'")
    print("")
    
    # Test Pydantic model directly
    print("🔧 Testing DatabaseSettings directly:")
    try:
        from shared.config.settings import DatabaseSettings
        
        # Create DatabaseSettings directly
        db_settings = DatabaseSettings()
        
        print(f"   terminus_url: '{db_settings.terminus_url}'")
        print(f"   terminus_user: '{db_settings.terminus_user}'")
        print(f"   terminus_account: '{db_settings.terminus_account}'")
        print(f"   terminus_password: '{db_settings.terminus_password}'")
        print("")
        
        # Test the Pydantic field configuration
        print("🔍 Pydantic Field Configuration:")
        for field_name, field_info in DatabaseSettings.model_fields.items():
            if 'terminus' in field_name:
                env_name = getattr(field_info, 'json_schema_extra', {}).get('env', 'No env set')
                if hasattr(field_info, 'default'):
                    default_val = field_info.default
                else:
                    default_val = 'No default'
                print(f"   {field_name}: env='{env_name}', default='{default_val}'")
        
    except Exception as e:
        print(f"   ❌ DatabaseSettings error: {e}")
        import traceback
        traceback.print_exc()
    
    print("")
    
    # Test full ApplicationSettings
    print("🔧 Testing Full ApplicationSettings:")
    try:
        from shared.config.settings import ApplicationSettings
        
        # Create ApplicationSettings directly  
        app_settings = ApplicationSettings()
        
        print(f"   database.terminus_url: '{app_settings.database.terminus_url}'")
        print(f"   database.terminus_user: '{app_settings.database.terminus_user}'") 
        print(f"   database.terminus_account: '{app_settings.database.terminus_account}'")
        print(f"   database.terminus_password: '{app_settings.database.terminus_password}'")
        
    except Exception as e:
        print(f"   ❌ ApplicationSettings error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_pydantic()