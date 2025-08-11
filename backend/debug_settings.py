#!/usr/bin/env python3
"""
Debug Application Settings
실제 settings에서 로드되는 값들을 확인
"""

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def debug_settings():
    print("🔍 Application Settings Debug")
    print("=" * 50)
    
    # Check environment variables
    import os
    print("📋 Environment Variables:")
    terminus_vars = {
        "TERMINUS_SERVER_URL": os.getenv("TERMINUS_SERVER_URL"),
        "TERMINUS_USER": os.getenv("TERMINUS_USER"),
        "TERMINUS_ACCOUNT": os.getenv("TERMINUS_ACCOUNT"),
        "TERMINUS_KEY": os.getenv("TERMINUS_KEY"),
    }
    
    for key, value in terminus_vars.items():
        print(f"   {key}: {value}")
    
    print("")
    
    # Check ApplicationSettings
    print("🔧 ApplicationSettings:")
    try:
        from shared.config.settings import settings
        
        print(f"   database.terminus_url: {settings.database.terminus_url}")
        print(f"   database.terminus_user: {settings.database.terminus_user}")
        print(f"   database.terminus_account: {settings.database.terminus_account}")
        print(f"   database.terminus_password: {settings.database.terminus_password}")
        print("")
        
        # Test connection config creation
        print("🔑 ConnectionConfig Creation:")
        from shared.models.config import ConnectionConfig
        
        connection_info = ConnectionConfig(
            server_url=settings.database.terminus_url,
            user=settings.database.terminus_user,
            account=settings.database.terminus_account,
            key=settings.database.terminus_password,
        )
        
        print(f"   server_url: {connection_info.server_url}")
        print(f"   user: {connection_info.user}")
        print(f"   account: {connection_info.account}")
        print(f"   key: {connection_info.key}")
        print("")
        
        # Test auth header generation
        import base64
        credentials = f"{connection_info.user}:{connection_info.key}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        auth_header = f"Basic {encoded_credentials}"
        
        print(f"   credentials: {credentials}")
        print(f"   auth_header: {auth_header}")
        
    except Exception as e:
        print(f"   ❌ Settings error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_settings()