"""
Comprehensive unit tests for app_logger module
Tests all logging functionality with REAL logging behavior verification
NO MOCKS - Tests actual logging system behavior
"""

import logging
import sys
import io
import pytest
from contextlib import redirect_stdout, redirect_stderr
from unittest.mock import patch

from shared.utils.app_logger import (
    get_logger,
    configure_logging,
    get_funnel_logger,
    get_bff_logger,
    get_oms_logger,
)


class TestGetLogger:
    """Test get_logger function with REAL logging behavior"""

    def test_get_logger_basic_creation(self):
        """Test basic logger creation with real logger instance"""
        logger_name = "test_basic_logger"
        logger = get_logger(logger_name)
        
        # Verify actual logger properties
        assert isinstance(logger, logging.Logger)
        assert logger.name == logger_name
        assert logger.level == logging.INFO  # Default level
        assert logger.propagate is False  # Should be disabled
        
    def test_get_logger_with_custom_level_string(self):
        """Test logger creation with custom level as string"""
        logger_name = "test_debug_logger"
        logger = get_logger(logger_name, level="DEBUG")
        
        assert logger.level == logging.DEBUG
        assert len(logger.handlers) > 0  # Should have handlers
        
    def test_get_logger_with_custom_level_constant(self):
        """Test logger creation with custom level as constant"""
        logger_name = "test_warning_logger"
        logger = get_logger(logger_name, level="WARNING")
        
        assert logger.level == logging.WARNING
        
    def test_get_logger_invalid_level_defaults_to_info(self):
        """Test that invalid log level defaults to INFO"""
        logger_name = "test_invalid_level_logger"
        logger = get_logger(logger_name, level="INVALID_LEVEL")
        
        assert logger.level == logging.INFO
        
    def test_get_logger_handler_configuration(self):
        """Test that logger has proper handler configuration"""
        logger_name = "test_handler_logger"
        logger = get_logger(logger_name)
        
        # Should have exactly one handler
        assert len(logger.handlers) == 1
        
        handler = logger.handlers[0]
        assert isinstance(handler, logging.StreamHandler)
        assert handler.stream == sys.stdout
        
        # Check formatter
        formatter = handler.formatter
        assert formatter is not None
        assert "%(asctime)s" in formatter._fmt
        assert "%(name)s" in formatter._fmt
        assert "%(levelname)s" in formatter._fmt
        assert "%(message)s" in formatter._fmt
        
    def test_get_logger_no_duplicate_handlers(self):
        """Test that calling get_logger multiple times doesn't add duplicate handlers"""
        logger_name = "test_duplicate_logger"
        
        logger1 = get_logger(logger_name)
        initial_handler_count = len(logger1.handlers)
        
        logger2 = get_logger(logger_name)
        final_handler_count = len(logger2.handlers)
        
        assert logger1 is logger2  # Should be same instance
        assert initial_handler_count == final_handler_count
        
    def test_get_logger_actual_logging_output(self):
        """Test actual logging output by capturing handler stream"""
        logger_name = "test_output_logger"
        logger = get_logger(logger_name, level="INFO")
        
        # Get the handler and replace its stream with StringIO
        handler = logger.handlers[0]
        original_stream = handler.stream
        captured_output = io.StringIO()
        handler.stream = captured_output
        
        try:
            logger.info("Test info message")
            logger.warning("Test warning message")
            logger.error("Test error message")
            
            output = captured_output.getvalue()
            
            # Verify actual log messages were written
            assert "Test info message" in output
            assert "Test warning message" in output
            assert "Test error message" in output
            assert logger_name in output
            assert "INFO" in output
            assert "WARNING" in output
            assert "ERROR" in output
        finally:
            # Restore original stream
            handler.stream = original_stream
        
    def test_get_logger_debug_level_filtering(self):
        """Test that log level filtering actually works"""
        logger_name = "test_filtering_logger"
        logger = get_logger(logger_name, level="WARNING")
        
        # Capture handler stream
        handler = logger.handlers[0]
        original_stream = handler.stream
        captured_output = io.StringIO()
        handler.stream = captured_output
        
        try:
            logger.debug("Debug message - should not appear")
            logger.info("Info message - should not appear")
            logger.warning("Warning message - should appear")
            logger.error("Error message - should appear")
            
            output = captured_output.getvalue()
            
            # Debug and info should be filtered out
            assert "Debug message" not in output
            assert "Info message" not in output
            
            # Warning and error should appear
            assert "Warning message" in output
            assert "Error message" in output
        finally:
            handler.stream = original_stream


class TestConfigureLogging:
    """Test configure_logging function"""

    def setup_method(self):
        """Record initial state for each test"""
        # Record initial handlers count (pytest may add its own)
        self.initial_handlers_count = len(logging.root.handlers)
        self.initial_level = logging.root.level

    def teardown_method(self):
        """Reset logging configuration after each test"""
        # Reset to initial level
        logging.root.setLevel(self.initial_level)

    def test_configure_logging_default_level(self):
        """Test configure_logging with default INFO level"""
        configure_logging()
        
        # Check that level was set correctly
        assert logging.root.level == logging.INFO
        
        # If no handlers existed initially, one should be added
        if self.initial_handlers_count == 0:
            assert len(logging.root.handlers) == 1
            # Test actual logging by capturing the new handler stream
            root_handler = logging.root.handlers[0]
            original_stream = root_handler.stream
            captured_output = io.StringIO()
            root_handler.stream = captured_output
            
            try:
                logging.info("Test info message")
                logging.warning("Test warning message")
                
                output = captured_output.getvalue()
                assert "Test info message" in output
                assert "Test warning message" in output
            finally:
                root_handler.stream = original_stream
        else:
            # If handlers existed, they should be preserved
            assert len(logging.root.handlers) == self.initial_handlers_count

    def test_configure_logging_debug_level(self):
        """Test configure_logging with DEBUG level"""
        configure_logging("DEBUG")
        
        assert logging.root.level == logging.DEBUG

    def test_configure_logging_error_level(self):
        """Test configure_logging with ERROR level"""
        configure_logging("ERROR")
        
        assert logging.root.level == logging.ERROR

    def test_configure_logging_invalid_level(self):
        """Test configure_logging with invalid level defaults to INFO"""
        configure_logging("INVALID_LEVEL")
        
        assert logging.root.level == logging.INFO

    def test_configure_logging_case_insensitive(self):
        """Test configure_logging is case insensitive"""
        configure_logging("debug")
        assert logging.root.level == logging.DEBUG
        
        configure_logging("Warning")
        assert logging.root.level == logging.WARNING
        
    def test_configure_logging_respects_existing_handlers(self):
        """Test that configure_logging doesn't add handlers when they exist"""
        initial_count = len(logging.root.handlers)
        
        configure_logging("DEBUG")
        
        # Should not add new handlers if they already exist
        assert len(logging.root.handlers) == initial_count
        assert logging.root.level == logging.DEBUG
        
    def test_configure_logging_adds_handler_when_none_exist(self):
        """Test that configure_logging adds handler when none exist"""
        # This test only runs if we can clean handlers (mainly for future tests)
        # Clear all handlers temporarily
        original_handlers = logging.root.handlers[:]
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        
        try:
            assert len(logging.root.handlers) == 0
            
            configure_logging("INFO")
            
            # Should add exactly one handler
            assert len(logging.root.handlers) == 1
            assert logging.root.level == logging.INFO
            
            # Verify the handler is properly configured
            handler = logging.root.handlers[0]
            assert isinstance(handler, logging.StreamHandler)
            assert handler.stream == sys.stdout
            assert handler.formatter is not None
            
        finally:
            # Restore original handlers
            for handler in logging.root.handlers[:]:
                logging.root.removeHandler(handler)
            for handler in original_handlers:
                logging.root.addHandler(handler)


class TestServiceLoggers:
    """Test service-specific logger functions"""

    def test_get_funnel_logger(self):
        """Test funnel logger creation and naming"""
        logger = get_funnel_logger()
        assert logger.name == "funnel.funnel"
        
        custom_logger = get_funnel_logger("data_processor")
        assert custom_logger.name == "funnel.data_processor"

    def test_get_bff_logger(self):
        """Test BFF logger creation and naming"""
        logger = get_bff_logger()
        assert logger.name == "bff.bff"
        
        custom_logger = get_bff_logger("router")
        assert custom_logger.name == "bff.router"

    def test_get_oms_logger(self):
        """Test OMS logger creation and naming"""
        logger = get_oms_logger()
        assert logger.name == "oms.oms"
        
        custom_logger = get_oms_logger("terminus")
        assert custom_logger.name == "oms.terminus"

    def test_service_logger_inheritance(self):
        """Test that service loggers inherit from get_logger functionality"""
        funnel_logger = get_funnel_logger("test")
        
        # Should have all the same properties as regular logger
        assert isinstance(funnel_logger, logging.Logger)
        assert len(funnel_logger.handlers) > 0
        assert funnel_logger.propagate is False
        
        # Test actual logging works
        handler = funnel_logger.handlers[0]
        original_stream = handler.stream
        captured_output = io.StringIO()
        handler.stream = captured_output
        
        try:
            funnel_logger.info("Funnel test message")
            
            output = captured_output.getvalue()
            assert "Funnel test message" in output
            assert "funnel.test" in output
        finally:
            handler.stream = original_stream

    def test_service_loggers_are_distinct(self):
        """Test that different service loggers are distinct instances"""
        funnel_logger = get_funnel_logger("test")
        bff_logger = get_bff_logger("test")
        oms_logger = get_oms_logger("test")
        
        # Should be different logger instances
        assert funnel_logger is not bff_logger
        assert bff_logger is not oms_logger
        assert funnel_logger is not oms_logger
        
        # Should have different names
        assert funnel_logger.name != bff_logger.name
        assert bff_logger.name != oms_logger.name
        assert funnel_logger.name != oms_logger.name


class TestLoggerIntegration:
    """Test integration scenarios with real logging"""

    def test_multiple_loggers_coexistence(self):
        """Test that multiple loggers can coexist without interference"""
        logger1 = get_logger("service1", "DEBUG")
        logger2 = get_logger("service2", "WARNING")
        logger3 = get_funnel_logger("component")
        
        # Capture outputs for each logger separately
        handler1 = logger1.handlers[0]
        handler2 = logger2.handlers[0]
        handler3 = logger3.handlers[0]
        
        original_stream1 = handler1.stream
        original_stream2 = handler2.stream
        original_stream3 = handler3.stream
        
        captured_output1 = io.StringIO()
        captured_output2 = io.StringIO()
        captured_output3 = io.StringIO()
        
        handler1.stream = captured_output1
        handler2.stream = captured_output2
        handler3.stream = captured_output3
        
        try:
            logger1.debug("Debug from service1")
            logger1.warning("Warning from service1")
            
            logger2.debug("Debug from service2 - should not appear")
            logger2.warning("Warning from service2")
            logger2.error("Error from service2")
            
            logger3.info("Info from funnel component")
            
            output1 = captured_output1.getvalue()
            output2 = captured_output2.getvalue()
            output3 = captured_output3.getvalue()
            
            # Check service1 messages (DEBUG level)
            assert "Debug from service1" in output1
            assert "Warning from service1" in output1
            
            # Check service2 messages (WARNING level, debug filtered)
            assert "Debug from service2" not in output2
            assert "Warning from service2" in output2
            assert "Error from service2" in output2
            
            # Check funnel messages
            assert "Info from funnel component" in output3
            assert "funnel.component" in output3
            
        finally:
            handler1.stream = original_stream1
            handler2.stream = original_stream2
            handler3.stream = original_stream3

    def test_logger_level_independence(self):
        """Test that different loggers maintain independent log levels"""
        debug_logger = get_logger("debug_service", "DEBUG")
        error_logger = get_logger("error_service", "ERROR")
        
        # Capture outputs separately
        debug_handler = debug_logger.handlers[0]
        error_handler = error_logger.handlers[0]
        
        original_debug_stream = debug_handler.stream
        original_error_stream = error_handler.stream
        
        debug_output = io.StringIO()
        error_output = io.StringIO()
        
        debug_handler.stream = debug_output
        error_handler.stream = error_output
        
        try:
            debug_logger.debug("Debug message")
            debug_logger.info("Info message")
            debug_logger.error("Error message")
            
            error_logger.debug("Debug should not appear")
            error_logger.info("Info should not appear")
            error_logger.warning("Warning should not appear")
            error_logger.error("Error should appear")
            
            debug_result = debug_output.getvalue()
            error_result = error_output.getvalue()
            
            # Debug logger should show all levels
            assert "Debug message" in debug_result
            assert "Info message" in debug_result
            assert "Error message" in debug_result
            
            # Error logger should only show error
            assert "Debug should not appear" not in error_result
            assert "Info should not appear" not in error_result
            assert "Warning should not appear" not in error_result
            assert "Error should appear" in error_result
            
        finally:
            debug_handler.stream = original_debug_stream
            error_handler.stream = original_error_stream

    def test_real_world_service_logging_scenario(self):
        """Test realistic service logging scenario"""
        # Simulate different service components
        api_logger = get_bff_logger("api")
        db_logger = get_oms_logger("database")
        processor_logger = get_funnel_logger("processor")
        
        # Capture all outputs
        api_handler = api_logger.handlers[0]
        db_handler = db_logger.handlers[0]
        processor_handler = processor_logger.handlers[0]
        
        original_api_stream = api_handler.stream
        original_db_stream = db_handler.stream
        original_processor_stream = processor_handler.stream
        
        api_output = io.StringIO()
        db_output = io.StringIO()
        processor_output = io.StringIO()
        
        api_handler.stream = api_output
        db_handler.stream = db_output
        processor_handler.stream = processor_output
        
        try:
            # Simulate API request processing
            api_logger.info("Received API request")
            api_logger.debug("Processing request parameters")
            
            # Simulate database operations
            db_logger.info("Connecting to database")
            db_logger.warning("Slow query detected")
            
            # Simulate data processing
            processor_logger.info("Starting data processing")
            processor_logger.error("Processing failed due to invalid data")
            
            # Simulate API response
            api_logger.error("Returning error response to client")
            
            api_result = api_output.getvalue()
            db_result = db_output.getvalue()
            processor_result = processor_output.getvalue()
            
            # Verify all service messages appear with correct service names
            assert "bff.api" in api_result
            assert "oms.database" in db_result
            assert "funnel.processor" in processor_result
            
            # Verify log levels and messages
            assert "Received API request" in api_result
            assert "Connecting to database" in db_result
            assert "Slow query detected" in db_result
            assert "Starting data processing" in processor_result
            assert "Processing failed" in processor_result
            assert "Returning error response" in api_result
            
        finally:
            api_handler.stream = original_api_stream
            db_handler.stream = original_db_stream
            processor_handler.stream = original_processor_stream


class TestLoggerEdgeCases:
    """Test edge cases and error conditions"""

    def test_logger_with_empty_name(self):
        """Test logger creation with empty name returns root logger"""
        logger = get_logger("")
        assert isinstance(logger, logging.Logger)
        # Empty string logger name actually returns root logger in Python logging
        assert logger.name == "root"

    def test_logger_with_none_level(self):
        """Test logger creation with None level"""
        logger = get_logger("test_none_level", level=None)
        assert logger.level == logging.INFO  # Should default to INFO

    def test_service_logger_with_empty_name(self):
        """Test service logger with empty component name"""
        funnel_logger = get_funnel_logger("")
        assert funnel_logger.name == "funnel."
        
        bff_logger = get_bff_logger("")
        assert bff_logger.name == "bff."
        
        oms_logger = get_oms_logger("")
        assert oms_logger.name == "oms."

    def test_logger_formatter_consistency(self):
        """Test that all loggers use consistent formatting"""
        loggers = [
            get_logger("test1"),
            get_funnel_logger("test2"),
            get_bff_logger("test3"),
            get_oms_logger("test4"),
        ]
        
        for logger in loggers:
            assert len(logger.handlers) > 0
            formatter = logger.handlers[0].formatter
            assert formatter is not None
            
            # All should use the same format
            expected_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            assert formatter._fmt == expected_format


if __name__ == "__main__":
    pytest.main([__file__])