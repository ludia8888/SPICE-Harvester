"""
🔥 THINK ULTRA!! Unified PYTHONPATH Configuration for Python Scripts
Provides consistent, dynamic PYTHONPATH setup for all Python scripts and tests
"""

import os
import sys
from pathlib import Path
from typing import Optional


def detect_backend_directory() -> Optional[Path]:
    """
    Dynamically detect the backend directory by looking for characteristic files/directories.
    
    Returns:
        Path to backend directory if found, None otherwise
    """
    # Start from the current file's directory
    current_path = Path(__file__).resolve()
    
    # Look for backend directory markers
    backend_markers = ["pyproject.toml", "shared", "bff", "oms", "funnel"]
    
    # Walk up the directory tree
    for parent in [current_path] + list(current_path.parents):
        # Check if all backend markers exist
        if all((parent / marker).exists() for marker in backend_markers):
            return parent
    
    # If not found, try alternative approach - look for specific subdirectories
    current_path = Path(__file__).resolve()
    for parent in [current_path] + list(current_path.parents):
        if (
            (parent / "shared").is_dir() and
            (parent / "bff").is_dir() and 
            (parent / "oms").is_dir() and
            (parent / "pyproject.toml").is_file()
        ):
            return parent
    
    return None


def setup_pythonpath(backend_dir: Optional[Path] = None) -> bool:
    """
    Setup PYTHONPATH to include the backend directory.
    
    Args:
        backend_dir: Optional backend directory path. If not provided, will auto-detect.
        
    Returns:
        True if setup successful, False otherwise
    """
    if backend_dir is None:
        backend_dir = detect_backend_directory()
    
    if backend_dir is None:
        print("❌ Could not detect backend directory automatically!", file=sys.stderr)
        print("Please ensure this script is run from within the SPICE HARVESTER backend project.", file=sys.stderr)
        return False
    
    if not backend_dir.exists():
        print(f"❌ Backend directory does not exist: {backend_dir}", file=sys.stderr)
        return False
    
    # Convert to string for sys.path operations
    backend_str = str(backend_dir)
    
    # Add to sys.path if not already present
    if backend_str not in sys.path:
        sys.path.insert(0, backend_str)
    
    # Also set PYTHONPATH environment variable for subprocesses
    current_pythonpath = os.environ.get('PYTHONPATH', '')
    if backend_str not in current_pythonpath:
        if current_pythonpath:
            os.environ['PYTHONPATH'] = f"{backend_str}:{current_pythonpath}"
        else:
            os.environ['PYTHONPATH'] = backend_str
    
    # Remove duplicates from PYTHONPATH
    pythonpath_parts = os.environ['PYTHONPATH'].split(':')
    unique_parts = []
    seen = set()
    for part in pythonpath_parts:
        if part and part not in seen:
            unique_parts.append(part)
            seen.add(part)
    os.environ['PYTHONPATH'] = ':'.join(unique_parts)
    
    return True


def validate_pythonpath() -> bool:
    """
    Validate that PYTHONPATH is correctly configured by testing imports.
    
    Returns:
        True if validation successful, False otherwise
    """
    try:
        # Test if we can import shared modules (use simpler import to avoid circular imports)
        import shared.models.common  # noqa: F401
        return True
    except ImportError as e:
        print(f"❌ Cannot import shared modules: {e}", file=sys.stderr)
        print(f"   Current sys.path: {sys.path[:3]}...", file=sys.stderr)
        print(f"   Current PYTHONPATH: {os.environ.get('PYTHONPATH', 'Not set')}", file=sys.stderr)
        return False


def configure_python_environment(backend_dir: Optional[Path] = None, verbose: bool = True) -> bool:
    """
    Complete Python environment configuration including PYTHONPATH setup and validation.
    
    Args:
        backend_dir: Optional backend directory path. If not provided, will auto-detect.
        verbose: Whether to print status messages
        
    Returns:
        True if configuration successful, False otherwise
    """
    if verbose:
        print("🔥 THINK ULTRA!! Configuring Python environment...")
        print("=" * 50)
    
    # Detect backend directory
    if backend_dir is None:
        backend_dir = detect_backend_directory()
    
    if backend_dir is None:
        if verbose:
            print("❌ Backend directory detection failed", file=sys.stderr)
        return False
    
    # Setup PYTHONPATH
    if not setup_pythonpath(backend_dir):
        if verbose:
            print("❌ PYTHONPATH setup failed", file=sys.stderr)
        return False
    
    if verbose:
        print(f"✅ PYTHONPATH configured successfully:")
        print(f"   Backend Directory: {backend_dir}")
        print(f"   PYTHONPATH: {os.environ.get('PYTHONPATH', 'Not set')}")
    
    # Validate setup
    if not validate_pythonpath():
        if verbose:
            print("❌ PYTHONPATH validation failed", file=sys.stderr)
        return False
    
    if verbose:
        print("✅ PYTHONPATH validation successful")
        print("🎉 Python environment configured successfully!")
        print("=" * 50)
    
    return True


def ensure_backend_in_path() -> None:
    """
    Convenience function to ensure backend directory is in Python path.
    This is meant to be called at the top of Python scripts that need shared modules.
    
    Raises:
        RuntimeError: If backend directory cannot be detected or configured
    """
    if not configure_python_environment(verbose=False):
        raise RuntimeError(
            "Failed to configure Python environment. "
            "Please ensure this script is run from within the SPICE HARVESTER backend project."
        )


if __name__ == "__main__":
    """Allow script to be run directly for testing/debugging."""
    success = configure_python_environment(verbose=True)
    sys.exit(0 if success else 1)