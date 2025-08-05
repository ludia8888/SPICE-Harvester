"""
Synchronous API Wrapper Service

비동기 Command를 동기적으로 실행하고 결과를 기다리는 서비스
"""

import asyncio
import logging
import time
from typing import Optional, Dict, Any, Callable
from datetime import datetime

from shared.models.sync_wrapper import SyncOptions, SyncResult, TimeoutError
from shared.models.commands import CommandStatus
from shared.services.command_status_service import CommandStatusService

logger = logging.getLogger(__name__)


class SyncWrapperService:
    """
    비동기 Command API를 동기적으로 래핑하는 서비스.
    
    Command를 제출하고 완료될 때까지 기다린 후 결과를 반환합니다.
    """
    
    def __init__(self, command_status_service: CommandStatusService):
        self.command_status_service = command_status_service
        
    async def wait_for_command(
        self,
        command_id: str,
        options: SyncOptions = SyncOptions()
    ) -> SyncResult:
        """
        Command가 완료될 때까지 기다리고 결과를 반환합니다.
        
        Args:
            command_id: 대기할 Command ID
            options: 실행 옵션
            
        Returns:
            SyncResult: 실행 결과
            
        Raises:
            TimeoutError: 타임아웃 발생 시
        """
        start_time = time.time()
        progress_history = [] if options.include_progress else None
        last_status = CommandStatus.PENDING
        retry_count = 0
        
        try:
            # asyncio.wait_for를 사용하여 타임아웃 구현
            result = await asyncio.wait_for(
                self._poll_until_complete(
                    command_id, 
                    options, 
                    progress_history,
                    start_time
                ),
                timeout=options.timeout
            )
            
            execution_time = time.time() - start_time
            
            # 성공 결과 생성
            return SyncResult(
                success=True,
                command_id=command_id,
                data=result.get("result"),
                execution_time=execution_time,
                final_status=result["status"],
                progress_history=progress_history,
                retry_count=retry_count
            )
            
        except asyncio.TimeoutError:
            # 타임아웃 처리
            execution_time = time.time() - start_time
            
            # 마지막 상태 확인
            last_details = await self.command_status_service.get_command_details(command_id)
            if last_details:
                last_status = last_details["status"]
                
            # 타임아웃 결과 반환
            return SyncResult(
                success=False,
                command_id=command_id,
                error=f"Command timed out after {options.timeout} seconds",
                execution_time=execution_time,
                final_status="TIMEOUT",
                progress_history=progress_history,
                retry_count=retry_count
            )
            
        except Exception as e:
            # 기타 에러 처리
            execution_time = time.time() - start_time
            logger.error(f"Error waiting for command {command_id}: {e}")
            
            return SyncResult(
                success=False,
                command_id=command_id,
                error=str(e),
                execution_time=execution_time,
                final_status="ERROR",
                progress_history=progress_history,
                retry_count=retry_count
            )
            
    async def _poll_until_complete(
        self,
        command_id: str,
        options: SyncOptions,
        progress_history: Optional[list],
        start_time: float
    ) -> Dict[str, Any]:
        """
        Command가 완료될 때까지 주기적으로 상태를 확인합니다.
        
        Args:
            command_id: Command ID
            options: 실행 옵션
            progress_history: 진행률 이력 리스트
            start_time: 시작 시간
            
        Returns:
            완료된 Command 상세 정보
        """
        while True:
            # 상태 확인
            details = await self.command_status_service.get_command_details(command_id)
            
            if not details:
                raise ValueError(f"Command {command_id} not found")
                
            current_status = details["status"]
            
            # 진행률 기록
            if progress_history is not None:
                progress_entry = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "elapsed_time": time.time() - start_time,
                    "status": current_status,
                    "progress": details.get("progress", 0)
                }
                
                # 진행 메시지가 있으면 포함
                if "progress_message" in details.get("data", {}):
                    progress_entry["message"] = details["data"]["progress_message"]
                    
                progress_history.append(progress_entry)
                
            # 완료 상태 확인
            if current_status == CommandStatus.COMPLETED:
                logger.info(f"Command {command_id} completed successfully")
                return details
                
            elif current_status == CommandStatus.FAILED:
                error_msg = details.get("error", "Command execution failed")
                
                # 재시도 옵션 확인
                if options.retry_on_failure and details.get("retry_count", 0) < options.max_retries:
                    logger.warning(f"Command {command_id} failed, waiting for retry...")
                    # 재시도를 기다림
                else:
                    raise Exception(error_msg)
                    
            elif current_status == CommandStatus.CANCELLED:
                raise Exception("Command was cancelled")
                
            # 다음 폴링까지 대기
            await asyncio.sleep(options.poll_interval)
            
    async def execute_sync(
        self,
        async_func: Callable,
        request_data: Dict[str, Any],
        options: SyncOptions = SyncOptions()
    ) -> SyncResult:
        """
        비동기 함수를 실행하고 결과를 기다립니다.
        
        Args:
            async_func: 실행할 비동기 함수 (CommandResult를 반환해야 함)
            request_data: 요청 데이터
            options: 실행 옵션
            
        Returns:
            SyncResult: 실행 결과
        """
        try:
            # 비동기 함수 실행하여 command_id 획득
            command_result = await async_func(**request_data)
            command_id = str(command_result.command_id)
            
            logger.info(f"Submitted command {command_id}, waiting for completion...")
            
            # Command 완료 대기
            return await self.wait_for_command(command_id, options)
            
        except Exception as e:
            logger.error(f"Failed to execute sync wrapper: {e}")
            
            # 실행 실패 결과 반환
            return SyncResult(
                success=False,
                command_id="",
                error=str(e),
                execution_time=0,
                final_status="ERROR",
                retry_count=0
            )
            
    async def get_command_progress(
        self,
        command_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Command의 현재 진행 상태를 조회합니다.
        
        Args:
            command_id: Command ID
            
        Returns:
            진행 상태 정보 또는 None
        """
        details = await self.command_status_service.get_command_details(command_id)
        
        if not details:
            return None
            
        return {
            "command_id": command_id,
            "status": details["status"],
            "progress": details.get("progress", 0),
            "progress_message": details.get("data", {}).get("progress_message"),
            "updated_at": details.get("updated_at")
        }