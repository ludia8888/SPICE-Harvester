#!/usr/bin/env python3
"""
Debug Application Settings
실제 settings에서 로드되는 값들을 확인
"""

from shared.config.settings import get_settings

def debug_settings():
    print("🔍 Application Settings Debug")
    print("=" * 50)

    def _mask_secret(value: str, *, show: int = 2) -> str:
        raw = str(value or "")
        if not raw:
            return ""
        if len(raw) <= show * 2:
            return "***"
        return f"{raw[:show]}***{raw[-show:]}"

    print("🔧 ApplicationSettings (SSoT):")
    try:
        settings = get_settings()
        
        print(f"   database.terminus_url: {settings.database.terminus_url}")
        print(f"   database.terminus_user: {settings.database.terminus_user}")
        print(f"   database.terminus_account: {settings.database.terminus_account}")
        print(f"   database.terminus_password: {_mask_secret(settings.database.terminus_password)}")
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
        print(f"   key: {_mask_secret(connection_info.key)}")
        print("")
        
        # Test auth header generation
        import base64
        credentials = f"{connection_info.user}:{connection_info.key}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        auth_header = f"Basic {encoded_credentials}"

        print("   credentials: <redacted>")
        print("   auth_header: <redacted>")
        
    except Exception as e:
        print(f"   ❌ Settings error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_settings()
