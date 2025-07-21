"""
Comprehensive unit tests for log_rotation module
Tests all log rotation functionality with REAL file system operations
"""

import os
import gzip
import tempfile
import pytest
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch, Mock

from shared.utils.log_rotation import (
    LogRotationManager,
    create_default_rotation_manager,
)


class TestLogRotationManager:
    """Test LogRotationManager class functionality"""

    def setup_method(self):
        """Setup test environment with temporary directory"""
        self.temp_dir = tempfile.mkdtemp()
        self.log_dir = Path(self.temp_dir) / "logs"
        self.manager = LogRotationManager(
            log_dir=str(self.log_dir),
            max_size_mb=1,  # Small for testing
            max_files=3,
            compress_after_days=0,  # Immediate compression for testing
            delete_after_days=2,
        )

    def teardown_method(self):
        """Clean up test environment"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_init_creates_log_directory(self):
        """Test that initialization creates log directory"""
        assert self.log_dir.exists()
        assert self.log_dir.is_dir()

    def test_init_with_existing_directory(self):
        """Test initialization with existing directory"""
        existing_dir = Path(self.temp_dir) / "existing_logs"
        existing_dir.mkdir(parents=True, exist_ok=True)
        
        manager = LogRotationManager(str(existing_dir))
        
        assert existing_dir.exists()
        assert manager.log_dir == existing_dir

    def test_init_parameters_stored_correctly(self):
        """Test that initialization parameters are stored correctly"""
        assert self.manager.max_size_bytes == 1 * 1024 * 1024
        assert self.manager.max_files == 3
        assert self.manager.compress_after_days == 0
        assert self.manager.delete_after_days == 2

    def test_get_file_size_existing_file(self):
        """Test getting size of existing file"""
        test_file = self.log_dir / "test.log"
        test_content = "This is a test log file"
        test_file.write_text(test_content)
        
        size = self.manager.get_file_size(test_file)
        
        assert size == len(test_content)

    def test_get_file_size_nonexistent_file(self):
        """Test getting size of non-existent file"""
        nonexistent_file = self.log_dir / "nonexistent.log"
        
        size = self.manager.get_file_size(nonexistent_file)
        
        assert size == 0

    def test_get_file_age_days_new_file(self):
        """Test getting age of newly created file"""
        test_file = self.log_dir / "new.log"
        test_file.write_text("test")
        
        age = self.manager.get_file_age_days(test_file)
        
        assert age < 0.1  # Less than 0.1 days (should be very recent)

    def test_get_file_age_days_nonexistent_file(self):
        """Test getting age of non-existent file"""
        nonexistent_file = self.log_dir / "nonexistent.log"
        
        age = self.manager.get_file_age_days(nonexistent_file)
        
        assert age == 0

    @patch('shared.utils.log_rotation.datetime')
    def test_get_file_age_days_old_file(self, mock_datetime):
        """Test getting age of old file"""
        test_file = self.log_dir / "old.log"
        test_file.write_text("test")
        
        # Mock current time to be 2 days later
        original_mtime = test_file.stat().st_mtime
        mock_datetime.now.return_value.timestamp.return_value = original_mtime + (2 * 24 * 3600)
        
        age = self.manager.get_file_age_days(test_file)
        
        assert abs(age - 2.0) < 0.1  # Should be approximately 2 days

    def test_should_rotate_small_file(self):
        """Test rotation check for small file"""
        test_file = self.log_dir / "small.log"
        test_file.write_text("small content")
        
        should_rotate = self.manager.should_rotate(test_file)
        
        assert should_rotate is False

    def test_should_rotate_large_file(self):
        """Test rotation check for large file"""
        test_file = self.log_dir / "large.log"
        # Create file larger than max_size_mb (1MB)
        large_content = "x" * (2 * 1024 * 1024)  # 2MB
        test_file.write_text(large_content)
        
        should_rotate = self.manager.should_rotate(test_file)
        
        assert should_rotate is True

    def test_should_rotate_nonexistent_file(self):
        """Test rotation check for non-existent file"""
        nonexistent_file = self.log_dir / "nonexistent.log"
        
        should_rotate = self.manager.should_rotate(nonexistent_file)
        
        assert should_rotate is False

    def test_rotate_log_file_success(self):
        """Test successful log file rotation"""
        test_file = self.log_dir / "service.log"
        test_content = "Original log content"
        test_file.write_text(test_content)
        
        result = self.manager.rotate_log_file(test_file, "service")
        
        assert result is True
        # Original file should still exist but be empty/new
        assert test_file.exists()
        # Rotated file should exist
        rotated_files = list(self.log_dir.glob("service_rotated_*.log"))
        assert len(rotated_files) == 1
        # Rotated file should contain original content
        assert rotated_files[0].read_text() == test_content

    def test_rotate_log_file_nonexistent(self):
        """Test rotation of non-existent file"""
        nonexistent_file = self.log_dir / "nonexistent.log"
        
        result = self.manager.rotate_log_file(nonexistent_file, "service")
        
        assert result is False

    def test_rotate_log_file_creates_new_file(self):
        """Test that rotation creates new empty log file"""
        test_file = self.log_dir / "service.log"
        test_file.write_text("content")
        
        self.manager.rotate_log_file(test_file, "service")
        
        # New file should exist and be empty
        assert test_file.exists()
        assert test_file.stat().st_size == 0

    def test_compress_old_logs_no_files(self):
        """Test compression when no files exist"""
        compressed_count = self.manager.compress_old_logs()
        
        assert compressed_count == 0

    def test_compress_old_logs_recent_files(self):
        """Test compression with recent files (shouldn't compress)"""
        # Create a recent rotated file
        recent_file = self.log_dir / "service_rotated_20250720_120000.log"
        recent_file.write_text("recent content")
        
        # Use manager with compress_after_days=1 to avoid immediate compression
        manager = LogRotationManager(
            str(self.log_dir),
            compress_after_days=1
        )
        
        compressed_count = manager.compress_old_logs()
        
        assert compressed_count == 0
        assert recent_file.exists()
        assert not (self.log_dir / "service_rotated_20250720_120000.log.gz").exists()

    def test_compress_old_logs_old_files(self):
        """Test compression of old files"""
        # Create an old rotated file
        old_file = self.log_dir / "service_rotated_20250718_120000.log"
        old_content = "old log content"
        old_file.write_text(old_content)
        
        # Set file modification time to be old
        old_timestamp = datetime.now().timestamp() - (2 * 24 * 3600)  # 2 days ago
        os.utime(old_file, (old_timestamp, old_timestamp))
        
        compressed_count = self.manager.compress_old_logs()
        
        assert compressed_count == 1
        assert not old_file.exists()  # Original should be deleted
        
        # Compressed file should exist
        compressed_file = self.log_dir / "service_rotated_20250718_120000.log.gz"
        assert compressed_file.exists()
        
        # Verify compressed content
        with gzip.open(compressed_file, 'rt') as f:
            assert f.read() == old_content

    def test_cleanup_old_logs_no_files(self):
        """Test cleanup when no files exist"""
        deleted_count, freed_mb = self.manager.cleanup_old_logs()
        
        assert deleted_count == 0
        assert freed_mb == 0

    def test_cleanup_old_logs_recent_files(self):
        """Test cleanup with recent files (shouldn't delete)"""
        recent_file = self.log_dir / "service_rotated_20250720_120000.log"
        recent_file.write_text("recent content")
        
        deleted_count, freed_mb = self.manager.cleanup_old_logs()
        
        assert deleted_count == 0
        assert freed_mb == 0
        assert recent_file.exists()

    def test_cleanup_old_logs_very_old_files(self):
        """Test cleanup of very old files"""
        # Create old files
        old_file = self.log_dir / "service_rotated_20250710_120000.log"
        old_file.write_text("old content")
        
        old_compressed = self.log_dir / "service_rotated_20250711_120000.log.gz"
        with gzip.open(old_compressed, 'wt') as f:
            f.write("old compressed content")
        
        # Set modification times to be very old
        very_old_timestamp = datetime.now().timestamp() - (5 * 24 * 3600)  # 5 days ago
        os.utime(old_file, (very_old_timestamp, very_old_timestamp))
        os.utime(old_compressed, (very_old_timestamp, very_old_timestamp))
        
        deleted_count, freed_mb = self.manager.cleanup_old_logs()
        
        assert deleted_count == 2
        assert freed_mb > 0
        assert not old_file.exists()
        assert not old_compressed.exists()

    def test_limit_rotated_files_within_limit(self):
        """Test file limiting when within limit"""
        # Create files within limit
        for i in range(2):
            file_path = self.log_dir / f"service_rotated_2025072{i}_120000.log"
            file_path.write_text(f"content {i}")
        
        removed_count = self.manager.limit_rotated_files("service")
        
        assert removed_count == 0

    def test_limit_rotated_files_exceeds_limit(self):
        """Test file limiting when exceeding limit"""
        # Create more files than limit (max_files=3)
        for i in range(5):
            file_path = self.log_dir / f"service_rotated_2025072{i}_120000.log"
            file_path.write_text(f"content {i}")
            # Set different modification times
            timestamp = datetime.now().timestamp() - (i * 3600)  # i hours ago
            os.utime(file_path, (timestamp, timestamp))
        
        removed_count = self.manager.limit_rotated_files("service")
        
        assert removed_count == 2  # Should remove 2 oldest files
        
        # Verify only 3 files remain
        remaining_files = list(self.log_dir.glob("service_rotated_*.log"))
        assert len(remaining_files) == 3

    def test_limit_rotated_files_mixed_compressed(self):
        """Test file limiting with mixed compressed and uncompressed files"""
        # Create mix of compressed and uncompressed files
        for i in range(4):
            if i % 2 == 0:
                file_path = self.log_dir / f"service_rotated_2025072{i}_120000.log"
                file_path.write_text(f"content {i}")
            else:
                file_path = self.log_dir / f"service_rotated_2025072{i}_120000.log.gz"
                with gzip.open(file_path, 'wt') as f:
                    f.write(f"compressed content {i}")
            
            # Set different modification times
            timestamp = datetime.now().timestamp() - (i * 3600)
            os.utime(file_path, (timestamp, timestamp))
        
        removed_count = self.manager.limit_rotated_files("service")
        
        assert removed_count == 1  # Should remove 1 oldest file
        
        # Verify 3 files remain (mixed types)
        all_files = list(self.log_dir.glob("service_rotated_*"))
        assert len(all_files) == 3

    def test_get_log_directory_info_empty(self):
        """Test directory info for empty directory"""
        info = self.manager.get_log_directory_info()
        
        assert info["directory"] == str(self.log_dir)
        assert info["total_files"] == 0
        assert info["total_size_mb"] == 0
        assert info["exists"] is True

    def test_get_log_directory_info_with_files(self):
        """Test directory info with files"""
        # Create test files
        file1 = self.log_dir / "test1.log"
        file1.write_text("content1")
        
        file2 = self.log_dir / "test2.log"
        file2.write_text("longer content for file2")
        
        info = self.manager.get_log_directory_info()
        
        assert info["total_files"] == 2
        assert info["total_size_mb"] > 0
        assert info["exists"] is True

    def test_get_log_directory_info_error_handling(self):
        """Test directory info error handling"""
        # Use an existing manager but mock the directory operations to fail
        with patch.object(Path, 'rglob', side_effect=OSError("Permission denied")):
            info = self.manager.get_log_directory_info()
            
            assert "error" in info
            assert "Permission denied" in info["error"]


class TestPerformMaintenance:
    """Test the complete maintenance workflow"""

    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.log_dir = Path(self.temp_dir) / "logs"
        self.manager = LogRotationManager(
            log_dir=str(self.log_dir),
            max_size_mb=1,  # 1MB for testing
            max_files=2,
            compress_after_days=0,  # Immediate compression
            delete_after_days=1,  # Delete after 1 day
        )

    def teardown_method(self):
        """Clean up test environment"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_perform_maintenance_no_files(self):
        """Test maintenance with no files"""
        stats = self.manager.perform_maintenance(["oms", "bff"])
        
        assert stats["rotated_files"] == 0
        assert stats["compressed_files"] == 0
        assert stats["deleted_files"] == 0
        assert stats["freed_space_mb"] == 0
        assert stats["limited_files"] == 0
        assert len(stats["errors"]) == 0

    def test_perform_maintenance_full_workflow(self):
        """Test complete maintenance workflow"""
        # Create large files that need rotation
        large_content = "x" * (2 * 1024 * 1024)  # 2MB
        oms_file = self.log_dir / "oms_20250720_120000.log"
        bff_file = self.log_dir / "bff_20250720_120000.log"
        
        oms_file.write_text(large_content)
        bff_file.write_text(large_content)
        
        # Create old rotated files for compression
        old_rotated = self.log_dir / "oms_rotated_20250718_120000.log"
        old_rotated.write_text("old content")
        old_timestamp = datetime.now().timestamp() - (2 * 24 * 3600)
        os.utime(old_rotated, (old_timestamp, old_timestamp))
        
        # Create very old files for deletion
        very_old = self.log_dir / "bff_rotated_20250715_120000.log.gz"
        with gzip.open(very_old, 'wt') as f:
            f.write("very old content")
        very_old_timestamp = datetime.now().timestamp() - (5 * 24 * 3600)
        os.utime(very_old, (very_old_timestamp, very_old_timestamp))
        
        stats = self.manager.perform_maintenance(["oms", "bff"])
        
        # Should have rotated the large files
        assert stats["rotated_files"] == 2
        
        # Should have compressed old files
        assert stats["compressed_files"] >= 1
        
        # Should have deleted very old files
        assert stats["deleted_files"] >= 1
        assert stats["freed_space_mb"] > 0
        
        # No errors expected
        assert len(stats["errors"]) == 0

    def test_perform_maintenance_with_errors(self):
        """Test maintenance handling errors gracefully"""
        # Create a file with problematic permissions to cause errors
        problem_file = self.log_dir / "oms_20250720_120000.log"
        problem_file.write_text("content")
        
        # Mock an operation to fail
        with patch.object(self.manager, 'rotate_log_file', side_effect=Exception("Test error")):
            stats = self.manager.perform_maintenance(["oms"])
        
        # Should capture errors but not crash
        assert isinstance(stats["errors"], list)

    def test_perform_maintenance_skips_rotated_files(self):
        """Test that maintenance skips already rotated files"""
        # Create a normal file and an already rotated file
        normal_file = self.log_dir / "oms_20250720_120000.log"
        normal_file.write_text("x" * (2 * 1024 * 1024))  # Large
        
        rotated_file = self.log_dir / "oms_rotated_20250720_120000.log"
        rotated_file.write_text("x" * (2 * 1024 * 1024))  # Large but already rotated
        
        stats = self.manager.perform_maintenance(["oms"])
        
        # Should only rotate the normal file, not the already rotated one
        assert stats["rotated_files"] == 1

    def test_perform_maintenance_handles_multiple_services(self):
        """Test maintenance with multiple services"""
        services = ["oms", "bff", "funnel"]
        
        # Create files for each service
        for service in services:
            file_path = self.log_dir / f"{service}_20250720_120000.log"
            file_path.write_text("x" * (2 * 1024 * 1024))  # Large file
        
        stats = self.manager.perform_maintenance(services)
        
        assert stats["rotated_files"] == len(services)


class TestCreateDefaultRotationManager:
    """Test the default rotation manager factory function"""

    def test_create_default_rotation_manager(self):
        """Test creating default rotation manager"""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = create_default_rotation_manager(temp_dir)
            
            assert isinstance(manager, LogRotationManager)
            assert manager.max_size_bytes == 5 * 1024 * 1024  # 5MB
            assert manager.max_files == 5
            assert manager.compress_after_days == 1
            assert manager.delete_after_days == 7

    def test_create_default_rotation_manager_creates_directory(self):
        """Test that default manager creates directory"""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_dir = Path(temp_dir) / "new_logs"
            
            manager = create_default_rotation_manager(str(log_dir))
            
            assert log_dir.exists()
            assert log_dir.is_dir()


class TestLogRotationIntegration:
    """Test integration scenarios"""

    def setup_method(self):
        """Setup integration test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.log_dir = Path(self.temp_dir) / "logs"

    def teardown_method(self):
        """Clean up integration test environment"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_realistic_log_rotation_scenario(self):
        """Test realistic log rotation scenario"""
        manager = LogRotationManager(
            log_dir=str(self.log_dir),
            max_size_mb=1,  # Small for quick testing
            max_files=3,
            compress_after_days=0,  # Immediate compression
            delete_after_days=1,
        )
        
        # Simulate daily log creation and rotation
        services = ["oms", "bff"]
        
        for day in range(5):  # Simulate 5 days
            for service in services:
                # Create daily log file
                log_file = self.log_dir / f"{service}_day{day}.log"
                log_content = f"Day {day} logs for {service}\n" * 10000  # Large content
                log_file.write_text(log_content)
                
                # Perform maintenance
                stats = manager.perform_maintenance([service])
                
                # Verify maintenance was performed
                assert isinstance(stats, dict)
                assert "errors" in stats
        
        # After 5 days, verify cleanup happened
        final_info = manager.get_log_directory_info()
        
        # Should have cleaned up old files
        assert final_info["total_files"] < 20  # Much less than 5 days * 2 services * multiple files

    def test_concurrent_service_maintenance(self):
        """Test maintenance for multiple services concurrently"""
        manager = create_default_rotation_manager(str(self.log_dir))
        
        # Create logs for multiple services
        services = ["oms", "bff", "funnel", "data_connector"]
        
        for service in services:
            for i in range(3):
                log_file = self.log_dir / f"{service}_{i}.log"
                log_file.write_text(f"Service {service} log {i}\n" * 10000)
        
        # Perform maintenance on all services
        stats = manager.perform_maintenance(services)
        
        # Verify maintenance handled all services
        assert isinstance(stats, dict)
        assert len(stats["errors"]) == 0

    def test_edge_case_empty_service_list(self):
        """Test maintenance with empty service list"""
        manager = create_default_rotation_manager(str(self.log_dir))
        
        stats = manager.perform_maintenance([])
        
        assert stats["rotated_files"] == 0
        assert len(stats["errors"]) == 0

    def test_edge_case_nonexistent_service(self):
        """Test maintenance with non-existent service"""
        manager = create_default_rotation_manager(str(self.log_dir))
        
        stats = manager.perform_maintenance(["nonexistent_service"])
        
        # Should handle gracefully
        assert isinstance(stats, dict)
        assert len(stats["errors"]) == 0


class TestErrorHandlingAndEdgeCases:
    """Test error handling and edge cases"""

    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.log_dir = Path(self.temp_dir) / "logs"

    def teardown_method(self):
        """Clean up test environment"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_permission_error_handling(self):
        """Test handling of permission errors"""
        manager = LogRotationManager(str(self.log_dir))
        
        # Create a file that might cause permission issues
        test_file = self.log_dir / "test.log"
        test_file.write_text("content")
        
        # Mock os.chmod to raise permission error
        with patch('os.chmod', side_effect=PermissionError("Permission denied")):
            result = manager.rotate_log_file(test_file, "service")
            
            # Should handle error gracefully
            assert result is False

    def test_disk_space_handling(self):
        """Test handling when disk space is low"""
        manager = LogRotationManager(str(self.log_dir))
        
        # Mock shutil.move to raise OSError (disk space)
        with patch('shutil.move', side_effect=OSError("No space left on device")):
            test_file = self.log_dir / "test.log"
            test_file.write_text("content")
            
            result = manager.rotate_log_file(test_file, "service")
            
            assert result is False

    def test_corrupted_file_handling(self):
        """Test handling of corrupted files during compression"""
        manager = LogRotationManager(str(self.log_dir), compress_after_days=0)
        
        # Create a file that will be processed for compression
        test_file = self.log_dir / "service_rotated_20250718_120000.log"
        test_file.write_text("content")
        
        # Set old timestamp
        old_timestamp = datetime.now().timestamp() - (2 * 24 * 3600)
        os.utime(test_file, (old_timestamp, old_timestamp))
        
        # Mock gzip.open to raise error
        with patch('gzip.open', side_effect=OSError("Compression failed")):
            compressed_count = manager.compress_old_logs()
            
            # Should handle error gracefully
            assert compressed_count == 0

    def test_invalid_timestamp_in_filename(self):
        """Test handling files with invalid timestamps in filenames"""
        manager = LogRotationManager(str(self.log_dir))
        
        # Create files with invalid timestamp formats
        invalid_files = [
            "service_rotated_invalid_timestamp.log",
            "service_rotated_20250732_250000.log",  # Invalid date
            "service_rotated_.log",
        ]
        
        for filename in invalid_files:
            file_path = self.log_dir / filename
            file_path.write_text("content")
        
        # Should handle gracefully without crashing
        try:
            stats = manager.perform_maintenance(["service"])
            assert isinstance(stats, dict)
        except Exception as e:
            pytest.fail(f"Should handle invalid filenames gracefully: {e}")


if __name__ == "__main__":
    pytest.main([__file__])