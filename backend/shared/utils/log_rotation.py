"""
🔥 THINK ULTRA! Log Rotation Manager for Test Services
Prevents log files from growing too large and consuming excessive disk space
"""

import os
import glob
import gzip
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple, Optional
import stat


class LogRotationManager:
    """🔥 THINK ULTRA! Professional log rotation with compression and cleanup"""
    
    def __init__(
        self,
        log_dir: str,
        max_size_mb: int = 10,  # Maximum size per log file in MB
        max_files: int = 10,     # Maximum number of rotated files to keep
        compress_after_days: int = 1,  # Compress logs older than N days
        delete_after_days: int = 30,   # Delete logs older than N days
    ):
        self.log_dir = Path(log_dir)
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.max_files = max_files
        self.compress_after_days = compress_after_days
        self.delete_after_days = delete_after_days
        
        # Ensure log directory exists
        self.log_dir.mkdir(parents=True, exist_ok=True)
    
    def get_file_size(self, file_path: Path) -> int:
        """Get file size in bytes, handling errors gracefully"""
        try:
            return file_path.stat().st_size
        except (OSError, FileNotFoundError):
            return 0
    
    def get_file_age_days(self, file_path: Path) -> float:
        """Get file age in days"""
        try:
            mtime = file_path.stat().st_mtime
            age_seconds = datetime.now().timestamp() - mtime
            return age_seconds / (24 * 3600)  # Convert to days
        except (OSError, FileNotFoundError):
            return 0
    
    def should_rotate(self, log_file: Path) -> bool:
        """Check if log file should be rotated based on size"""
        if not log_file.exists():
            return False
        
        size = self.get_file_size(log_file)
        return size >= self.max_size_bytes
    
    def rotate_log_file(self, log_file: Path, service_name: str) -> bool:
        """
        Rotate a log file by renaming it with timestamp and creating new one
        
        Returns:
            True if rotation successful, False otherwise
        """
        if not log_file.exists():
            return False
        
        try:
            # Create rotation timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Generate rotated filename
            base_name = log_file.stem  # e.g., 'oms_20250720_140532'
            rotated_name = f"{service_name}_rotated_{timestamp}.log"
            rotated_path = self.log_dir / rotated_name
            
            # Move current log to rotated name
            shutil.move(str(log_file), str(rotated_path))
            
            # Create new empty log file with proper permissions
            log_file.touch()
            os.chmod(log_file, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
            
            print(f"🔄 Rotated {log_file.name} -> {rotated_name}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to rotate {log_file}: {e}")
            return False
    
    def compress_old_logs(self) -> int:
        """
        Compress log files older than compress_after_days
        
        Returns:
            Number of files compressed
        """
        compressed_count = 0
        
        # Find uncompressed rotated log files
        pattern = str(self.log_dir / "*_rotated_*.log")
        log_files = glob.glob(pattern)
        
        for log_file_str in log_files:
            log_file = Path(log_file_str)
            age_days = self.get_file_age_days(log_file)
            
            if age_days >= self.compress_after_days:
                try:
                    # Create compressed filename
                    compressed_path = log_file.with_suffix('.log.gz')
                    
                    # Compress the file
                    with open(log_file, 'rb') as f_in:
                        with gzip.open(compressed_path, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    
                    # Remove original file
                    log_file.unlink()
                    
                    print(f"🗜️  Compressed {log_file.name} -> {compressed_path.name}")
                    compressed_count += 1
                    
                except Exception as e:
                    print(f"❌ Failed to compress {log_file}: {e}")
        
        return compressed_count
    
    def cleanup_old_logs(self) -> Tuple[int, int]:
        """
        Remove log files older than delete_after_days
        
        Returns:
            Tuple of (deleted_files_count, freed_space_mb)
        """
        deleted_count = 0
        freed_space = 0
        
        # Find all log files (both compressed and uncompressed)
        patterns = [
            str(self.log_dir / "*_rotated_*.log"),
            str(self.log_dir / "*_rotated_*.log.gz")
        ]
        
        for pattern in patterns:
            log_files = glob.glob(pattern)
            
            for log_file_str in log_files:
                log_file = Path(log_file_str)
                age_days = self.get_file_age_days(log_file)
                
                if age_days >= self.delete_after_days:
                    try:
                        file_size = self.get_file_size(log_file)
                        log_file.unlink()
                        
                        freed_space += file_size
                        deleted_count += 1
                        print(f"🗑️  Deleted old log: {log_file.name} (age: {age_days:.1f} days)")
                        
                    except Exception as e:
                        print(f"❌ Failed to delete {log_file}: {e}")
        
        freed_space_mb = freed_space / (1024 * 1024)
        return deleted_count, freed_space_mb
    
    def limit_rotated_files(self, service_name: str) -> int:
        """
        Ensure we don't exceed max_files limit for rotated logs
        
        Returns:
            Number of files removed
        """
        removed_count = 0
        
        # Find all rotated files for this service (compressed and uncompressed)
        patterns = [
            str(self.log_dir / f"{service_name}_rotated_*.log"),
            str(self.log_dir / f"{service_name}_rotated_*.log.gz")
        ]
        
        all_files = []
        for pattern in patterns:
            all_files.extend(glob.glob(pattern))
        
        # Sort by modification time (newest first)
        all_files.sort(key=lambda x: Path(x).stat().st_mtime, reverse=True)
        
        # Remove excess files
        if len(all_files) > self.max_files:
            files_to_remove = all_files[self.max_files:]
            
            for file_path_str in files_to_remove:
                try:
                    file_path = Path(file_path_str)
                    file_path.unlink()
                    removed_count += 1
                    print(f"🗑️  Removed excess rotated file: {file_path.name}")
                    
                except Exception as e:
                    print(f"❌ Failed to remove {file_path_str}: {e}")
        
        return removed_count
    
    def perform_maintenance(self, service_logs: List[str]) -> dict:
        """
        Perform complete log maintenance: rotation, compression, cleanup
        
        Args:
            service_logs: List of service names to check (e.g., ['oms', 'bff'])
            
        Returns:
            Dictionary with maintenance statistics
        """
        print(f"🔧 Starting log maintenance at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        stats = {
            "rotated_files": 0,
            "compressed_files": 0,
            "deleted_files": 0,
            "freed_space_mb": 0,
            "limited_files": 0,
            "errors": []
        }
        
        try:
            # 1. Check for log rotation needs
            for service_name in service_logs:
                # Find current log files for this service
                pattern = str(self.log_dir / f"{service_name}_*.log")
                current_logs = glob.glob(pattern)
                
                for log_file_str in current_logs:
                    log_file = Path(log_file_str)
                    
                    # Skip already rotated files
                    if "_rotated_" in log_file.name:
                        continue
                    
                    if self.should_rotate(log_file):
                        if self.rotate_log_file(log_file, service_name):
                            stats["rotated_files"] += 1
            
            # 2. Compress old logs
            stats["compressed_files"] = self.compress_old_logs()
            
            # 3. Clean up very old logs
            deleted, freed_mb = self.cleanup_old_logs()
            stats["deleted_files"] = deleted
            stats["freed_space_mb"] = freed_mb
            
            # 4. Limit number of rotated files per service
            for service_name in service_logs:
                stats["limited_files"] += self.limit_rotated_files(service_name)
            
        except Exception as e:
            error_msg = f"Maintenance error: {e}"
            stats["errors"].append(error_msg)
            print(f"❌ {error_msg}")
        
        # Print summary
        print("\n" + "=" * 60)
        print("🔧 Log Maintenance Summary:")
        print(f"   Rotated files: {stats['rotated_files']}")
        print(f"   Compressed files: {stats['compressed_files']}")
        print(f"   Deleted files: {stats['deleted_files']}")
        print(f"   Freed space: {stats['freed_space_mb']:.2f} MB")
        print(f"   Limited files: {stats['limited_files']}")
        
        if stats["errors"]:
            print(f"   Errors: {len(stats['errors'])}")
            for error in stats["errors"]:
                print(f"     - {error}")
        
        print("=" * 60)
        
        return stats
    
    def get_log_directory_info(self) -> dict:
        """Get information about the log directory"""
        try:
            total_size = 0
            file_count = 0
            
            for file_path in self.log_dir.rglob("*"):
                if file_path.is_file():
                    total_size += self.get_file_size(file_path)
                    file_count += 1
            
            return {
                "directory": str(self.log_dir),
                "total_files": file_count,
                "total_size_mb": total_size / (1024 * 1024),
                "exists": self.log_dir.exists(),
            }
            
        except Exception as e:
            return {
                "directory": str(self.log_dir),
                "error": str(e),
                "exists": False,
            }


def create_default_rotation_manager(log_dir: str) -> LogRotationManager:
    """Create log rotation manager with sensible defaults for test services"""
    return LogRotationManager(
        log_dir=log_dir,
        max_size_mb=5,      # Rotate when files exceed 5MB
        max_files=5,        # Keep max 5 rotated files per service
        compress_after_days=1,  # Compress files older than 1 day
        delete_after_days=7,    # Delete files older than 7 days
    )


if __name__ == "__main__":
    """Allow direct execution for testing log rotation"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python log_rotation.py <log_directory> [service_names...]")
        print("Example: python log_rotation.py ./logs oms bff")
        sys.exit(1)
    
    log_dir = sys.argv[1]
    services = sys.argv[2:] if len(sys.argv) > 2 else ["oms", "bff"]
    
    print(f"🔥 THINK ULTRA! Running log rotation for {log_dir}")
    
    manager = create_default_rotation_manager(log_dir)
    
    # Show directory info before maintenance
    info = manager.get_log_directory_info()
    print(f"📁 Log directory: {info['directory']}")
    print(f"📊 Files: {info.get('total_files', 0)}, Size: {info.get('total_size_mb', 0):.2f} MB")
    
    # Perform maintenance
    stats = manager.perform_maintenance(services)
    
    # Show directory info after maintenance
    info_after = manager.get_log_directory_info()
    print(f"📊 After maintenance - Files: {info_after.get('total_files', 0)}, Size: {info_after.get('total_size_mb', 0):.2f} MB")
    
    sys.exit(0 if len(stats["errors"]) == 0 else 1)