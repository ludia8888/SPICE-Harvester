"""
Command Status Tracking Service

Manages command lifecycle states and provides status tracking functionality.
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum
from uuid import UUID

from shared.services.redis_service import RedisService
from shared.config.app_config import AppConfig

logger = logging.getLogger(__name__)


class CommandStatus(str, Enum):
    """Command execution status enumeration."""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    RETRYING = "RETRYING"


class CommandStatusService:
    """
    Service for tracking command execution status.
    
    Provides:
    - Status lifecycle management
    - Progress tracking
    - Result storage
    - Status history
    - Real-time status updates via Redis pub/sub
    """
    
    def __init__(self, redis_service: RedisService):
        self.redis = redis_service
        self.default_ttl = 86400  # 24 hours
        
    async def create_command_status(
        self,
        command_id: str,
        command_type: str,
        aggregate_id: str,
        payload: Dict[str, Any],
        user_id: Optional[str] = None
    ) -> None:
        """
        Create initial command status entry.
        
        Args:
            command_id: Unique command identifier
            command_type: Type of command (e.g., CREATE_ONTOLOGY_CLASS)
            aggregate_id: ID of the aggregate being modified
            payload: Command payload
            user_id: Optional user ID who initiated the command
        """
        data = {
            "command_type": command_type,
            "aggregate_id": aggregate_id,
            "payload": payload,
            "user_id": user_id,
            "created_at": datetime.utcnow().isoformat(),
            "history": [
                {
                    "status": CommandStatus.PENDING,
                    "timestamp": datetime.utcnow().isoformat(),
                    "message": "Command created"
                }
            ]
        }
        
        await self.redis.set_command_status(
            command_id,
            CommandStatus.PENDING,
            data,
            self.default_ttl
        )
        
        logger.info(f"Created command status for {command_id}")
        
    async def update_status(
        self,
        command_id: str,
        status: CommandStatus,
        message: Optional[str] = None,
        error: Optional[str] = None,
        progress: Optional[int] = None
    ) -> bool:
        """
        Update command status.
        
        Args:
            command_id: Unique command identifier
            status: New status
            message: Optional status message
            error: Optional error message (for FAILED status)
            progress: Optional progress percentage (0-100)
            
        Returns:
            True if updated successfully, False otherwise
        """
        current_data = await self.redis.get_command_status(command_id)
        
        if not current_data:
            logger.warning(f"Command {command_id} not found")
            return False
            
        # Add to history
        history_entry = {
            "status": status,
            "timestamp": datetime.utcnow().isoformat(),
            "message": message or f"Status changed to {status}"
        }
        
        if error:
            history_entry["error"] = error
            
        current_data["data"]["history"].append(history_entry)
        
        # Update progress if provided
        if progress is not None:
            current_data["data"]["progress"] = progress
            
        # Update error if provided
        if error:
            current_data["data"]["error"] = error
            
        # Store updated status
        await self.redis.set_command_status(
            command_id,
            status,
            current_data["data"],
            self.default_ttl
        )
        
        logger.info(f"Updated command {command_id} status to {status}")
        return True
        
    async def start_processing(
        self,
        command_id: str,
        worker_id: Optional[str] = None
    ) -> bool:
        """
        Mark command as being processed.
        
        Args:
            command_id: Unique command identifier
            worker_id: Optional worker ID processing the command
            
        Returns:
            True if updated successfully, False otherwise
        """
        message = "Processing started"
        if worker_id:
            message += f" by worker {worker_id}"
            
        return await self.update_status(
            command_id,
            CommandStatus.PROCESSING,
            message=message
        )
        
    async def complete_command(
        self,
        command_id: str,
        result: Dict[str, Any],
        message: Optional[str] = None
    ) -> bool:
        """
        Mark command as completed with result.
        
        Args:
            command_id: Unique command identifier
            result: Command execution result
            message: Optional completion message
            
        Returns:
            True if updated successfully, False otherwise
        """
        # Store result separately
        await self.redis.set_command_result(command_id, result, self.default_ttl)
        
        # Update status
        return await self.update_status(
            command_id,
            CommandStatus.COMPLETED,
            message=message or "Command completed successfully",
            progress=100
        )
        
    async def fail_command(
        self,
        command_id: str,
        error: str,
        retry_count: Optional[int] = None
    ) -> bool:
        """
        Mark command as failed.
        
        Args:
            command_id: Unique command identifier
            error: Error message
            retry_count: Number of retries attempted
            
        Returns:
            True if updated successfully, False otherwise
        """
        message = "Command execution failed"
        if retry_count is not None:
            message += f" after {retry_count} retries"
            
        return await self.update_status(
            command_id,
            CommandStatus.FAILED,
            message=message,
            error=error
        )
        
    async def cancel_command(
        self,
        command_id: str,
        reason: Optional[str] = None
    ) -> bool:
        """
        Cancel a pending or processing command.
        
        Args:
            command_id: Unique command identifier
            reason: Optional cancellation reason
            
        Returns:
            True if cancelled successfully, False otherwise
        """
        current_data = await self.redis.get_command_status(command_id)
        
        if not current_data:
            return False
            
        current_status = current_data["status"]
        
        # Can only cancel pending or processing commands
        if current_status not in [CommandStatus.PENDING, CommandStatus.PROCESSING]:
            logger.warning(
                f"Cannot cancel command {command_id} with status {current_status}"
            )
            return False
            
        return await self.update_status(
            command_id,
            CommandStatus.CANCELLED,
            message=reason or "Command cancelled by user"
        )
        
    async def get_command_details(
        self,
        command_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get complete command details including status and result.
        
        Args:
            command_id: Unique command identifier
            
        Returns:
            Command details or None if not found
        """
        status_data = await self.redis.get_command_status(command_id)
        
        if not status_data:
            return None
            
        details = {
            "command_id": command_id,
            "status": status_data["status"],
            "updated_at": status_data["updated_at"],
            **status_data["data"]
        }
        
        # Include result if completed
        if status_data["status"] == CommandStatus.COMPLETED:
            result = await self.redis.get_command_result(command_id)
            if result:
                details["result"] = result
                
        return details
        
    async def list_user_commands(
        self,
        user_id: str,
        status_filter: Optional[CommandStatus] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List commands for a specific user.
        
        Args:
            user_id: User ID
            status_filter: Optional status filter
            limit: Maximum number of commands to return
            
        Returns:
            List of command summaries
        """
        # Pattern to match user commands
        pattern = AppConfig.get_command_status_pattern()
        keys = await self.redis.keys(pattern)
        
        commands = []
        for key in keys[-limit:]:  # Get latest commands
            command_id = key.split(":")[1]
            details = await self.get_command_details(command_id)
            
            if details and details.get("user_id") == user_id:
                if not status_filter or details["status"] == status_filter:
                    # Create summary
                    summary = {
                        "command_id": command_id,
                        "status": details["status"],
                        "command_type": details.get("command_type"),
                        "created_at": details.get("created_at"),
                        "updated_at": details.get("updated_at"),
                        "progress": details.get("progress", 0)
                    }
                    commands.append(summary)
                    
        # Sort by created_at descending
        commands.sort(
            key=lambda x: x.get("created_at", ""),
            reverse=True
        )
        
        return commands[:limit]
        
    async def cleanup_old_commands(self, days: int = 7) -> int:
        """
        Clean up commands older than specified days.
        
        Args:
            days: Number of days to keep commands
            
        Returns:
            Number of commands cleaned up
        """
        # This would be implemented with a scheduled job
        # For now, Redis TTL handles automatic cleanup
        logger.info(f"Cleanup requested for commands older than {days} days")
        return 0