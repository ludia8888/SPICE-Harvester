"""
Deprecation utilities for marking obsolete methods
"""

import functools
import warnings
from typing import Optional, Callable
import logging

logger = logging.getLogger(__name__)


def deprecated(
    reason: str,
    version: Optional[str] = None,
    alternative: Optional[str] = None,
    removal_version: Optional[str] = None
) -> Callable:
    """
    Decorator to mark functions/methods as deprecated.
    
    Args:
        reason: Reason for deprecation
        version: Version when deprecated (e.g., "1.2.0")
        alternative: Suggested alternative method/function
        removal_version: Version when it will be removed
    
    Example:
        @deprecated(
            reason="Use execute_query() instead",
            version="1.0.0",
            alternative="execute_query",
            removal_version="2.0.0"
        )
        def query_database():
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            _issue_deprecation_warning(func, reason, version, alternative, removal_version)
            return await func(*args, **kwargs)
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            _issue_deprecation_warning(func, reason, version, alternative, removal_version)
            return func(*args, **kwargs)
        
        # Choose wrapper based on function type
        import asyncio
        if asyncio.iscoroutinefunction(func):
            wrapper = async_wrapper
        else:
            wrapper = sync_wrapper
        
        # Add deprecation metadata
        wrapper.__deprecated__ = True
        wrapper.__deprecation_reason__ = reason
        wrapper.__deprecation_version__ = version
        wrapper.__deprecation_alternative__ = alternative
        wrapper.__deprecation_removal_version__ = removal_version
        
        # Update docstring
        doc = func.__doc__ or ""
        deprecation_notice = f"\n\n.. deprecated:: {version or 'Unknown'}\n   {reason}"
        if alternative:
            deprecation_notice += f"\n   Use :func:`{alternative}` instead."
        if removal_version:
            deprecation_notice += f"\n   Will be removed in version {removal_version}."
        
        wrapper.__doc__ = doc + deprecation_notice
        
        return wrapper
    
    return decorator


def _issue_deprecation_warning(
    func: Callable,
    reason: str,
    version: Optional[str],
    alternative: Optional[str],
    removal_version: Optional[str]
):
    """Issue a deprecation warning with detailed information."""
    func_name = f"{func.__module__}.{func.__qualname__}"
    
    # Build warning message
    message = f"{func_name} is deprecated"
    if version:
        message += f" since version {version}"
    message += f". {reason}"
    if alternative:
        message += f" Use {alternative} instead."
    if removal_version:
        message += f" It will be removed in version {removal_version}."
    
    # Log and warn
    logger.warning(f"DeprecationWarning: {message}")
    warnings.warn(message, DeprecationWarning, stacklevel=3)


def legacy_api(
    reason: str = "This is a legacy API"
) -> Callable:
    """
    Decorator for legacy APIs that are kept for backward compatibility.
    Less severe than @deprecated - doesn't issue warnings by default.
    
    Args:
        reason: Explanation of why this is legacy
    """
    def decorator(func: Callable) -> Callable:
        # Just add metadata, don't issue warnings
        func.__legacy__ = True
        func.__legacy_reason__ = reason
        
        # Update docstring
        doc = func.__doc__ or ""
        legacy_notice = f"\n\n.. note:: Legacy API\n   {reason}"
        func.__doc__ = doc + legacy_notice
        
        return func
    
    return decorator


def experimental(
    feature: str = "This feature"
) -> Callable:
    """
    Decorator for experimental features that may change.
    
    Args:
        feature: Description of the experimental feature
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            logger.info(f"Using experimental feature: {feature}")
            return await func(*args, **kwargs)
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            logger.info(f"Using experimental feature: {feature}")
            return func(*args, **kwargs)
        
        # Choose wrapper based on function type
        import asyncio
        if asyncio.iscoroutinefunction(func):
            wrapper = async_wrapper
        else:
            wrapper = sync_wrapper
        
        # Add experimental metadata
        wrapper.__experimental__ = True
        wrapper.__experimental_feature__ = feature
        
        # Update docstring
        doc = func.__doc__ or ""
        experimental_notice = f"\n\n.. warning:: Experimental\n   {feature} is experimental and may change in future versions."
        wrapper.__doc__ = doc + experimental_notice
        
        return wrapper
    
    return decorator