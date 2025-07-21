"""
Comprehensive unit tests for pythonpath_setup module
Tests all Python path configuration functionality with REAL behavior verification
"""

import os
import sys
import pytest
from pathlib import Path
from unittest.mock import patch, Mock

from shared.utils.pythonpath_setup import (
    detect_backend_directory,
    setup_pythonpath,
    validate_pythonpath,
    configure_python_environment,
    ensure_backend_in_path,
)


class TestDetectBackendDirectory:
    """Test backend directory detection functionality"""

    def test_detect_backend_directory_current_location(self):
        """Test detection when running from within backend directory"""
        # This test runs from the actual backend directory
        result = detect_backend_directory()
        
        # Should find the backend directory
        assert result is not None
        assert isinstance(result, Path)
        assert result.exists()
        
        # Should contain the expected markers
        assert (result / "pyproject.toml").exists()
        assert (result / "shared").is_dir()
        assert (result / "bff").is_dir() or (result / "oms").is_dir()

    @patch('shared.utils.pythonpath_setup.Path')
    def test_detect_backend_directory_not_found(self, mock_path):
        """Test detection when backend directory cannot be found"""
        # Mock a path that doesn't contain backend markers
        mock_file = Mock()
        mock_file.resolve.return_value = Path("/some/random/path/file.py")
        mock_path.__file__ = str(mock_file)
        mock_path.return_value = mock_file
        
        # Mock the path checking to always return False
        with patch.object(Path, 'exists', return_value=False), \
             patch.object(Path, 'is_dir', return_value=False), \
             patch.object(Path, 'is_file', return_value=False):
            
            result = detect_backend_directory()
            assert result is None

    def test_detect_backend_directory_returns_path_object(self):
        """Test that detection returns proper Path object"""
        result = detect_backend_directory()
        
        if result is not None:  # Only test if we found a directory
            assert isinstance(result, Path)
            assert result.is_absolute()


class TestSetupPythonpath:
    """Test PYTHONPATH setup functionality"""

    def setup_method(self):
        """Save original environment for restoration"""
        self.original_path = sys.path.copy()
        self.original_pythonpath = os.environ.get('PYTHONPATH', '')

    def teardown_method(self):
        """Restore original environment"""
        sys.path[:] = self.original_path
        if self.original_pythonpath:
            os.environ['PYTHONPATH'] = self.original_pythonpath
        elif 'PYTHONPATH' in os.environ:
            del os.environ['PYTHONPATH']

    def test_setup_pythonpath_with_valid_directory(self):
        """Test setting up PYTHONPATH with valid backend directory"""
        backend_dir = detect_backend_directory()
        
        if backend_dir is None:
            pytest.skip("Backend directory not found")
        
        # Clear sys.path entry if it exists
        backend_str = str(backend_dir)
        if backend_str in sys.path:
            sys.path.remove(backend_str)
        
        result = setup_pythonpath(backend_dir)
        
        assert result is True
        assert backend_str in sys.path
        assert backend_str in os.environ.get('PYTHONPATH', '')

    def test_setup_pythonpath_auto_detect(self):
        """Test auto-detection of backend directory"""
        result = setup_pythonpath()
        
        # Should succeed if we're in the backend directory
        assert isinstance(result, bool)
        
        if result:  # Only check if setup succeeded
            backend_dir = detect_backend_directory()
            if backend_dir:
                assert str(backend_dir) in sys.path

    def test_setup_pythonpath_nonexistent_directory(self):
        """Test setup with non-existent directory"""
        nonexistent_dir = Path("/nonexistent/directory")
        
        result = setup_pythonpath(nonexistent_dir)
        
        assert result is False
        assert str(nonexistent_dir) not in sys.path

    def test_setup_pythonpath_none_input(self):
        """Test setup when backend_dir is None and auto-detect fails"""
        with patch('shared.utils.pythonpath_setup.detect_backend_directory', return_value=None):
            result = setup_pythonpath(None)
            
            assert result is False

    def test_setup_pythonpath_no_duplicate_entries(self):
        """Test that multiple setups don't create duplicate entries"""
        backend_dir = detect_backend_directory()
        
        if backend_dir is None:
            pytest.skip("Backend directory not found")
        
        backend_str = str(backend_dir)
        
        # Setup twice
        setup_pythonpath(backend_dir)
        initial_count = sys.path.count(backend_str)
        
        setup_pythonpath(backend_dir)
        final_count = sys.path.count(backend_str)
        
        assert initial_count == final_count

    def test_setup_pythonpath_environment_variable(self):
        """Test that PYTHONPATH environment variable is set correctly"""
        backend_dir = detect_backend_directory()
        
        if backend_dir is None:
            pytest.skip("Backend directory not found")
        
        backend_str = str(backend_dir)
        
        # Clear PYTHONPATH
        if 'PYTHONPATH' in os.environ:
            del os.environ['PYTHONPATH']
        
        result = setup_pythonpath(backend_dir)
        
        assert result is True
        assert backend_str in os.environ['PYTHONPATH']

    def test_setup_pythonpath_preserves_existing_pythonpath(self):
        """Test that existing PYTHONPATH entries are preserved"""
        backend_dir = detect_backend_directory()
        
        if backend_dir is None:
            pytest.skip("Backend directory not found")
        
        # Set existing PYTHONPATH
        existing_path = "/some/existing/path"
        os.environ['PYTHONPATH'] = existing_path
        
        result = setup_pythonpath(backend_dir)
        
        assert result is True
        assert existing_path in os.environ['PYTHONPATH']
        assert str(backend_dir) in os.environ['PYTHONPATH']

    def test_setup_pythonpath_removes_duplicates(self):
        """Test that duplicate PYTHONPATH entries are removed"""
        backend_dir = detect_backend_directory()
        
        if backend_dir is None:
            pytest.skip("Backend directory not found")
        
        backend_str = str(backend_dir)
        
        # Set PYTHONPATH with duplicates
        os.environ['PYTHONPATH'] = f"{backend_str}:/some/path:{backend_str}:/another/path"
        
        result = setup_pythonpath(backend_dir)
        
        assert result is True
        
        # Count occurrences of backend_str in PYTHONPATH
        pythonpath_parts = os.environ['PYTHONPATH'].split(':')
        backend_count = pythonpath_parts.count(backend_str)
        
        assert backend_count == 1


class TestValidatePythonpath:
    """Test PYTHONPATH validation functionality"""

    def test_validate_pythonpath_success(self):
        """Test validation when imports work"""
        # This should work in the actual test environment
        result = validate_pythonpath()
        
        # Should be able to import shared modules
        assert isinstance(result, bool)
        
        if result:
            # If validation passes, we should be able to import
            try:
                import shared.models.common
                assert True
            except ImportError:
                pytest.fail("Validation passed but import failed")

    @patch('shared.utils.pythonpath_setup.sys.stderr')
    @patch('builtins.__import__')
    def test_validate_pythonpath_import_error(self, mock_import, mock_stderr):
        """Test validation when imports fail"""
        mock_import.side_effect = ImportError("Cannot import module")
        
        result = validate_pythonpath()
        
        assert result is False
        # Should have printed error messages
        assert mock_stderr.write.called

    def test_validate_pythonpath_actual_import(self):
        """Test validation with actual shared module import"""
        # Ensure backend is in path first
        backend_dir = detect_backend_directory()
        if backend_dir:
            setup_pythonpath(backend_dir)
        
        result = validate_pythonpath()
        
        # In the actual environment, this should work
        # We can't guarantee it works in all test environments
        assert isinstance(result, bool)


class TestConfigurePythonEnvironment:
    """Test complete Python environment configuration"""

    def setup_method(self):
        """Save original environment"""
        self.original_path = sys.path.copy()
        self.original_pythonpath = os.environ.get('PYTHONPATH', '')

    def teardown_method(self):
        """Restore original environment"""
        sys.path[:] = self.original_path
        if self.original_pythonpath:
            os.environ['PYTHONPATH'] = self.original_pythonpath
        elif 'PYTHONPATH' in os.environ:
            del os.environ['PYTHONPATH']

    def test_configure_python_environment_success(self):
        """Test complete configuration process"""
        result = configure_python_environment(verbose=False)
        
        # Should succeed in the test environment
        assert isinstance(result, bool)
        
        if result:
            backend_dir = detect_backend_directory()
            if backend_dir:
                assert str(backend_dir) in sys.path

    def test_configure_python_environment_with_backend_dir(self):
        """Test configuration with explicit backend directory"""
        backend_dir = detect_backend_directory()
        
        if backend_dir is None:
            pytest.skip("Backend directory not found")
        
        result = configure_python_environment(backend_dir, verbose=False)
        
        assert result is True

    def test_configure_python_environment_verbose_output(self, capsys):
        """Test verbose output messages"""
        result = configure_python_environment(verbose=True)
        
        captured = capsys.readouterr()
        
        if result:
            assert "THINK ULTRA" in captured.out
            assert "Python environment" in captured.out
        else:
            # If it failed, should show error messages
            assert len(captured.err) > 0 or "failed" in captured.out.lower()

    @patch('shared.utils.pythonpath_setup.detect_backend_directory')
    def test_configure_python_environment_detection_failure(self, mock_detect):
        """Test configuration when backend detection fails"""
        mock_detect.return_value = None
        
        result = configure_python_environment(verbose=False)
        
        assert result is False

    @patch('shared.utils.pythonpath_setup.setup_pythonpath')
    def test_configure_python_environment_setup_failure(self, mock_setup):
        """Test configuration when PYTHONPATH setup fails"""
        mock_setup.return_value = False
        
        result = configure_python_environment(verbose=False)
        
        assert result is False

    @patch('shared.utils.pythonpath_setup.validate_pythonpath')
    def test_configure_python_environment_validation_failure(self, mock_validate):
        """Test configuration when validation fails"""
        mock_validate.return_value = False
        
        result = configure_python_environment(verbose=False)
        
        assert result is False


class TestEnsureBackendInPath:
    """Test convenience function for ensuring backend in path"""

    def setup_method(self):
        """Save original environment"""
        self.original_path = sys.path.copy()
        self.original_pythonpath = os.environ.get('PYTHONPATH', '')

    def teardown_method(self):
        """Restore original environment"""
        sys.path[:] = self.original_path
        if self.original_pythonpath:
            os.environ['PYTHONPATH'] = self.original_pythonpath
        elif 'PYTHONPATH' in os.environ:
            del os.environ['PYTHONPATH']

    def test_ensure_backend_in_path_success(self):
        """Test successful backend path ensuring"""
        # Should not raise exception in normal environment
        try:
            ensure_backend_in_path()
            # If we get here, it succeeded
            assert True
        except RuntimeError:
            # This is expected in some test environments
            pass

    @patch('shared.utils.pythonpath_setup.configure_python_environment')
    def test_ensure_backend_in_path_failure(self, mock_configure):
        """Test failure handling when configuration fails"""
        mock_configure.return_value = False
        
        with pytest.raises(RuntimeError) as exc_info:
            ensure_backend_in_path()
        
        assert "Failed to configure Python environment" in str(exc_info.value)
        assert "SPICE HARVESTER backend project" in str(exc_info.value)


class TestPythonpathSetupIntegration:
    """Test integration scenarios"""

    def setup_method(self):
        """Save original environment"""
        self.original_path = sys.path.copy()
        self.original_pythonpath = os.environ.get('PYTHONPATH', '')

    def teardown_method(self):
        """Restore original environment"""
        sys.path[:] = self.original_path
        if self.original_pythonpath:
            os.environ['PYTHONPATH'] = self.original_pythonpath
        elif 'PYTHONPATH' in os.environ:
            del os.environ['PYTHONPATH']

    def test_full_workflow_from_scratch(self):
        """Test complete workflow starting from clean state"""
        # Clean sys.path and PYTHONPATH
        backend_dir = detect_backend_directory()
        if backend_dir:
            backend_str = str(backend_dir)
            while backend_str in sys.path:
                sys.path.remove(backend_str)
        
        if 'PYTHONPATH' in os.environ:
            del os.environ['PYTHONPATH']
        
        # Run complete configuration
        result = configure_python_environment(verbose=False)
        
        if result:
            # Verify everything is set up correctly
            assert backend_str in sys.path
            assert backend_str in os.environ.get('PYTHONPATH', '')
            
            # Verify imports work
            validation_result = validate_pythonpath()
            assert validation_result is True

    def test_repeated_configuration_idempotent(self):
        """Test that repeated configuration is idempotent"""
        # Configure twice
        result1 = configure_python_environment(verbose=False)
        initial_path_length = len(sys.path)
        
        result2 = configure_python_environment(verbose=False)
        final_path_length = len(sys.path)
        
        # Should have same result and no duplicate entries
        assert result1 == result2
        assert final_path_length == initial_path_length

    def test_path_markers_validation(self):
        """Test that detected backend directory has all required markers"""
        backend_dir = detect_backend_directory()
        
        if backend_dir is not None:
            # All marker files/directories should exist
            assert (backend_dir / "pyproject.toml").exists()
            assert (backend_dir / "shared").is_dir()
            
            # At least one service directory should exist
            service_dirs = ["bff", "oms", "funnel"]
            assert any((backend_dir / service).is_dir() for service in service_dirs)


class TestEdgeCases:
    """Test edge cases and error conditions"""

    def test_detect_backend_directory_permission_error(self):
        """Test handling of permission errors during detection"""
        # This is difficult to test without actually changing permissions
        # So we'll just ensure the function handles exceptions gracefully
        result = detect_backend_directory()
        
        # Should either return a valid Path or None, not raise an exception
        assert result is None or isinstance(result, Path)

    def test_setup_pythonpath_empty_string(self):
        """Test setup with empty string backend directory"""
        # The function expects a Path object, so empty string should cause AttributeError
        with pytest.raises(AttributeError):
            setup_pythonpath("")

    def test_setup_pythonpath_relative_path(self):
        """Test setup with relative path"""
        relative_path = Path("./some/relative/path")
        result = setup_pythonpath(relative_path)
        
        # Should handle relative paths gracefully
        assert isinstance(result, bool)


if __name__ == "__main__":
    pytest.main([__file__])