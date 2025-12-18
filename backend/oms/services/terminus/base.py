"""
Base TerminusDB Service
기본 연결, 인증 및 공통 기능 제공
"""

import logging
import json
from typing import Any, Dict, Optional, Union
import httpx
from functools import wraps

from shared.models.config import ConnectionConfig
from shared.utils.terminus_branch import encode_branch_name
from oms.exceptions import ConnectionError as TerminusConnectionError

logger = logging.getLogger(__name__)


def async_terminus_retry(max_retries: int = 3, delay: float = 1.0):
    """비동기 재시도 데코레이터"""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            import asyncio
            
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except (httpx.ConnectError, httpx.TimeoutException) as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        await asyncio.sleep(delay * (attempt + 1))
                        logger.warning(
                            f"Retry {attempt + 1}/{max_retries} for {func.__name__}: {e}"
                        )
                    else:
                        logger.error(f"Max retries reached for {func.__name__}: {e}")
                except Exception as e:
                    # 다른 예외는 즉시 발생
                    raise

            # 모든 재시도 실패
            raise TerminusConnectionError(f"연결 실패 (재시도 {max_retries}회): {last_exception}")

        return wrapper

    return decorator


class BaseTerminusService:
    """
    TerminusDB 기본 서비스
    
    모든 TerminusDB 서비스의 기본 클래스로 연결, 인증, HTTP 요청 등의
    공통 기능을 제공합니다.
    """
    
    def __init__(self, connection_info: Optional[ConnectionConfig] = None):
        """
        TerminusDB 서비스 초기화
        
        Args:
            connection_info: 연결 정보 (없으면 환경변수에서 로드)
        """
        if connection_info:
            self.connection_info = connection_info
        else:
            # 환경변수에서 연결 정보 로드
            import os
            self.connection_info = ConnectionConfig(
                # Default to 6363 (TerminusDB default in this project); override via TERMINUS_SERVER_URL as needed.
                server_url=os.getenv("TERMINUS_SERVER_URL", "http://localhost:6363"),
                account=os.getenv("TERMINUS_ACCOUNT", "admin"),
                user=os.getenv("TERMINUS_USER", "admin"),
                key=os.getenv("TERMINUS_KEY", "admin"),
                ssl_verify=os.getenv("TERMINUS_SSL_VERIFY", "true").lower() == "true",
            )
        
        # HTTP 클라이언트 설정
        self._client: Optional[httpx.AsyncClient] = None
        self._auth_token: Optional[str] = None
        
        # 연결 상태
        self._connected = False
        
        logger.info(
            f"BaseTerminusService initialized with endpoint: {self.connection_info.server_url}"
        )
    
    async def _get_client(self) -> httpx.AsyncClient:
        """HTTP 클라이언트 가져오기 (lazy initialization)"""
        if not self._client:
            # 타임아웃 설정
            timeout = httpx.Timeout(
                connect=10.0,  # 연결 타임아웃
                read=30.0,     # 읽기 타임아웃
                write=30.0,    # 쓰기 타임아웃
                pool=5.0       # 연결 풀 타임아웃
            )
            
            # 재시도 설정
            transport = httpx.AsyncHTTPTransport(
                retries=3,
                verify=self.connection_info.ssl_verify
            )
            
            self._client = httpx.AsyncClient(
                base_url=self.connection_info.server_url,
                timeout=timeout,
                transport=transport,
                follow_redirects=True,
                headers={
                    "User-Agent": "SPICE-HARVESTER/1.0",
                    "Accept": "application/json",
                }
            )
            
        return self._client

    def _branch_descriptor(self, branch: Optional[str]) -> str:
        """
        Build the TerminusDB descriptor path for a branch.

        TerminusDB branch names cannot contain `/` in the descriptor segment, so we encode
        Git-like names (e.g. "feature/foo") at the Terminus boundary only.

        For main branch we keep the legacy/default path for compatibility.
        """
        if not branch or branch == "main":
            return ""
        encoded = encode_branch_name(branch)
        return f"/local/branch/{encoded}"
    
    async def _authenticate(self) -> str:
        """TerminusDB 인증 토큰 획득"""
        if self._auth_token:
            return self._auth_token
            
        logger.info("Authenticating with TerminusDB...")
        
        # Basic Auth 사용
        import base64
        credentials = f"{self.connection_info.user}:{self.connection_info.key}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        self._auth_token = f"Basic {encoded_credentials}"
        
        logger.info(f"Authentication successful for user: {self.connection_info.user}")
        return self._auth_token
    
    @async_terminus_retry(max_retries=3)
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Union[Dict[str, Any], str]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> Any:
        """
        HTTP 요청 실행
        
        Args:
            method: HTTP 메서드 (GET, POST, PUT, DELETE 등)
            endpoint: API 엔드포인트
            data: 요청 바디 데이터
            params: 쿼리 파라미터
            headers: 추가 헤더
            
        Returns:
            응답 데이터 (JSON 또는 텍스트)
        """
        client = await self._get_client()
        
        # 헤더 설정
        request_headers: Dict[str, str] = {}
        # TerminusDB v12 can hang on DELETE requests that include `Content-Type: application/json`
        # while sending no body. Only set Content-Type when we actually send JSON payloads.
        if data is not None and method.upper() in {"POST", "PUT", "PATCH", "DELETE"}:
            request_headers["Content-Type"] = "application/json"
        
        # Only add auth if credentials are configured
        logger.debug(f"Auth check: user={self.connection_info.user}, has_key={bool(self.connection_info.key)}, not_anonymous={self.connection_info.user != 'anonymous'}")
        # TerminusDB allows empty password for admin user
        # Check if user is set and not anonymous (key can be empty string)
        if self.connection_info.user and self.connection_info.user != "anonymous":
            auth_token = await self._authenticate()
            request_headers["Authorization"] = auth_token
            logger.debug(f"Added Authorization header for user: {self.connection_info.user}")
        else:
            logger.debug("Skipping authentication - using anonymous access")
        if headers:
            request_headers.update(headers)
        
        # 요청 로깅
        logger.debug(f"{method} {endpoint}")
        if params:
            logger.debug(f"Params: {params}")
        
        try:
            # 요청 실행
            if method.upper() == "GET":
                response = await client.get(
                    endpoint, params=params, headers=request_headers, **kwargs
                )
            elif method.upper() == "POST":
                response = await client.post(
                    endpoint, json=data, params=params, headers=request_headers, **kwargs
                )
            elif method.upper() == "PUT":
                response = await client.put(
                    endpoint, json=data, params=params, headers=request_headers, **kwargs
                )
            elif method.upper() == "DELETE":
                response = await client.delete(
                    endpoint, params=params, headers=request_headers, **kwargs
                )
            elif method.upper() == "PATCH":
                response = await client.patch(
                    endpoint, json=data, params=params, headers=request_headers, **kwargs
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            # 응답 처리
            if response.status_code >= 400:
                error_msg = f"TerminusDB API error: {response.status_code}"
                try:
                    error_data = response.json()
                    error_msg += f" - {error_data}"
                except:
                    error_msg += f" - {response.text}"
                
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # 응답 파싱
            content_type = response.headers.get("content-type", "")
            if content_type.startswith("application/json"):
                # Handle JSONL (JSON Lines) format - multiple JSON objects separated by newlines
                response_text = response.text
                if not response_text or not response_text.strip():
                    # TerminusDB streaming endpoints often return an empty body (200 OK) when there are no results.
                    # Example: `/api/document/...` with `stream=true`.
                    if "stream=true" in content_type:
                        return []
                    return {}
                logger.debug(f"Response content-type: {content_type}")
                logger.debug(f"Response text preview: {response_text[:200]}...")
                
                if response_text and '\n' in response_text and not response_text.strip().startswith('['):
                    # This is JSONL format
                    logger.info("Detected JSONL format response")
                    try:
                        lines = response_text.strip().split('\n')
                        result = [json.loads(line) for line in lines if line.strip()]
                        logger.info(f"Successfully parsed {len(result)} JSONL entries")
                        return result
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse as JSONL: {e}")
                        # Fall back to regular JSON parsing
                        return response.json()
                else:
                    return response.json()
            else:
                return response.text
                
        except httpx.TimeoutException as e:
            logger.error(f"Request timeout: {e}")
            raise TerminusConnectionError(f"요청 시간 초과: {e}")
        except httpx.ConnectError as e:
            logger.error(f"Connection error: {e}")
            raise TerminusConnectionError(f"연결 실패: {e}")
        except Exception as e:
            logger.error(f"Request failed: {e}")
            raise
    
    async def connect(self, db_name: Optional[str] = None) -> None:
        """TerminusDB 연결"""
        try:
            # 연결 테스트 - 서버 정보 조회
            endpoint = "/api/info"
            info = await self._make_request("GET", endpoint)
            
            logger.info(f"Connected to TerminusDB: {info}")
            self._connected = True
            
            if db_name:
                logger.info(f"Default database set to: {db_name}")
                
        except Exception as e:
            logger.error(f"Failed to connect to TerminusDB: {e}")
            raise TerminusConnectionError(f"TerminusDB 연결 실패: {e}")
    
    async def disconnect(self) -> None:
        """연결 종료"""
        if self._client:
            await self._client.aclose()
            self._client = None
            self._auth_token = None
            self._connected = False
            logger.info("Disconnected from TerminusDB")
    
    async def check_connection(self) -> bool:
        """연결 상태 확인"""
        try:
            await self._make_request("GET", "/api/info")
            return True
        except Exception:
            return False
    
    async def __aenter__(self):
        """비동기 컨텍스트 매니저 진입"""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """비동기 컨텍스트 매니저 종료"""
        await self.disconnect()
    
    def is_connected(self) -> bool:
        """연결 상태 반환"""
        return self._connected
