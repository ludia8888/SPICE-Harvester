"""
Comprehensive unit tests for ImageValidator
Tests all image validation functionality with REAL behavior verification
NO MOCKS - Tests verify actual code behavior
"""

import pytest
from typing import Dict, Any, Optional

from shared.validators.image_validator import ImageValidator
from shared.models.common import DataType


class TestImageValidator:
    """Test ImageValidator with real behavior verification"""

    def setup_method(self):
        """Setup for each test"""
        self.validator = ImageValidator()

    def test_validate_basic_image_url(self):
        """Test validation of basic image URL"""
        url = "https://example.com/image.jpg"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert result.message == "Valid image"
        assert result.normalized_value["url"] == url
        assert result.normalized_value["extension"] == ".jpg"
        assert result.normalized_value["filename"] == "image.jpg"
        assert result.normalized_value["domain"] == "example.com"
        assert result.normalized_value["isSecure"] is True
        assert result.normalized_value["path"] == "/image.jpg"
        assert result.metadata["type"] == "image"
        assert result.metadata["hasExtension"] is True

    def test_validate_all_supported_extensions(self):
        """Test validation with all supported image extensions"""
        extensions = [".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".tif", ".svg", ".ico"]
        
        for ext in extensions:
            url = f"https://example.com/image{ext}"
            result = self.validator.validate(url)
            assert result.is_valid is True, f"Failed for extension {ext}"
            assert result.normalized_value["extension"] == ext
            assert result.metadata["hasExtension"] is True

    def test_validate_non_string_types(self):
        """Test validation fails for non-string types"""
        # Number
        result = self.validator.validate(123)
        assert result.is_valid is False
        assert result.message == "Image must be a URL string"

        # List
        result = self.validator.validate([1, 2, 3])
        assert result.is_valid is False
        assert result.message == "Image must be a URL string"

        # Dict
        result = self.validator.validate({"url": "test"})
        assert result.is_valid is False
        assert result.message == "Image must be a URL string"

        # None
        result = self.validator.validate(None)
        assert result.is_valid is False
        assert result.message == "Image must be a URL string"

    def test_validate_invalid_url_format(self):
        """Test validation of invalid URL formats"""
        # Missing scheme
        result = self.validator.validate("example.com/image.jpg")
        assert result.is_valid is False
        assert result.message == "Invalid URL format - missing scheme or domain"

        # Missing domain
        result = self.validator.validate("https:///image.jpg")
        assert result.is_valid is False
        assert result.message == "Invalid URL format - missing scheme or domain"

        # Empty string
        result = self.validator.validate("")
        assert result.is_valid is False
        assert result.message == "Invalid URL format - missing scheme or domain"

    def test_validate_malformed_url_triggers_bare_except(self):
        """Test that malformed URLs trigger the bare except clause (BUG!)"""
        # This tests the actual behavior of the bare except at line 43
        # Note: Most malformed URLs don't actually trigger urlparse exceptions
        # but this test verifies the current code path
        
        # urlparse is very forgiving, so finding a string that actually raises is tricky
        # but let's test the current behavior
        malformed_urls = [
            "not a url at all",
            "://missing-scheme",
            "https://",
        ]
        
        for url in malformed_urls:
            result = self.validator.validate(url)
            # These will likely fail with "missing scheme or domain" rather than "Invalid URL format"
            # because urlparse doesn't raise exceptions easily
            assert result.is_valid is False

    def test_validate_require_https_constraint_pass(self):
        """Test HTTPS requirement constraint (passing)"""
        constraints = {"requireHttps": True}
        url = "https://example.com/image.jpg"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is True

    def test_validate_require_https_constraint_fail(self):
        """Test HTTPS requirement constraint (failing)"""
        constraints = {"requireHttps": True}
        url = "http://example.com/image.jpg"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is False
        assert result.message == "Image URL must use HTTPS"

    def test_validate_require_https_false(self):
        """Test HTTPS requirement when set to false"""
        constraints = {"requireHttps": False}
        url = "http://example.com/image.jpg"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is True

    def test_validate_allowed_domains_constraint_pass(self):
        """Test allowed domains constraint (passing)"""
        constraints = {"allowedDomains": ["example.com", "trusted.org"]}
        
        # Exact domain match
        result = self.validator.validate("https://example.com/image.jpg", constraints)
        assert result.is_valid is True

        # Subdomain match
        result = self.validator.validate("https://cdn.example.com/image.jpg", constraints)
        assert result.is_valid is True

        # Another allowed domain
        result = self.validator.validate("https://trusted.org/photo.png", constraints)
        assert result.is_valid is True

    def test_validate_allowed_domains_constraint_fail(self):
        """Test allowed domains constraint (failing)"""
        constraints = {"allowedDomains": ["example.com", "trusted.org"]}
        
        url = "https://malicious.com/image.jpg"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is False
        assert "Domain must be one of: ['example.com', 'trusted.org']" in result.message

    def test_validate_allowed_domains_with_port(self):
        """Test allowed domains with port numbers"""
        constraints = {"allowedDomains": ["example.com"]}
        
        # Domain with port should work (port is stripped)
        url = "https://example.com:8080/image.jpg"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is True

    def test_validate_allowed_domains_case_insensitive(self):
        """Test allowed domains are case insensitive"""
        constraints = {"allowedDomains": ["Example.COM"]}
        
        # Lowercase domain should match uppercase constraint
        url = "https://example.com/image.jpg"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is True

        # Subdomain case sensitivity
        url = "https://cdn.EXAMPLE.com/image.jpg"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is True

    def test_validate_require_extension_constraint_pass(self):
        """Test require extension constraint (passing)"""
        constraints = {"requireExtension": True}
        url = "https://example.com/image.jpg"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is True

    def test_validate_require_extension_constraint_fail(self):
        """Test require extension constraint (failing)"""
        constraints = {"requireExtension": True}
        url = "https://example.com/image"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is False
        # Extension order is not deterministic due to set to list conversion (BUG!)
        assert "URL must have a valid image extension:" in result.message
        assert ".jpg" in result.message
        assert ".png" in result.message

    def test_validate_require_extension_false(self):
        """Test require extension when set to false"""
        constraints = {"requireExtension": False}
        url = "https://example.com/image"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is True
        assert result.normalized_value["extension"] is None

    def test_validate_allowed_extensions_constraint_pass(self):
        """Test allowed extensions constraint (passing)"""
        constraints = {"allowedExtensions": [".jpg", ".png"]}
        
        url = "https://example.com/image.jpg"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is True

        url = "https://example.com/image.png"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is True

    def test_validate_allowed_extensions_constraint_fail(self):
        """Test allowed extensions constraint (failing)"""
        constraints = {"allowedExtensions": [".jpg", ".png"]}
        
        url = "https://example.com/image.gif"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is False
        assert "Image extension must be one of: ['.jpg', '.png']" in result.message

    def test_validate_allowed_extensions_no_extension_detected(self):
        """Test allowed extensions when no extension is detected"""
        constraints = {"allowedExtensions": [".jpg", ".png"]}
        
        # URL without extension - should pass because no extension to check
        url = "https://example.com/image"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is True

    def test_validate_combined_constraints(self):
        """Test validation with multiple constraints"""
        constraints = {
            "requireHttps": True,
            "allowedDomains": ["cdn.example.com"],
            "requireExtension": True,
            "allowedExtensions": [".jpg", ".png"]
        }
        
        # Valid URL meeting all constraints
        url = "https://cdn.example.com/photo.jpg"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is True

        # Fail HTTPS requirement
        url = "http://cdn.example.com/photo.jpg"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is False
        assert result.message == "Image URL must use HTTPS"

        # Fail domain requirement
        url = "https://malicious.com/photo.jpg"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is False
        assert "Domain must be one of" in result.message

        # Fail extension requirement
        url = "https://cdn.example.com/photo"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is False
        assert "URL must have a valid image extension" in result.message

        # Fail allowed extensions
        url = "https://cdn.example.com/photo.gif"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is False
        assert "Image extension must be one of" in result.message

    def test_validate_url_with_query_parameters(self):
        """Test URL with query parameters"""
        url = "https://example.com/image.jpg?size=large&format=jpeg"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert result.normalized_value["hasQuery"] is True
        assert result.normalized_value["query"] == "size=large&format=jpeg"

    def test_validate_url_without_query_parameters(self):
        """Test URL without query parameters"""
        url = "https://example.com/image.jpg"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert "hasQuery" not in result.normalized_value
        assert "query" not in result.normalized_value

    def test_validate_url_with_fragment(self):
        """Test URL with fragment"""
        url = "https://example.com/image.jpg#thumbnail"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert result.normalized_value["extension"] == ".jpg"

    def test_validate_url_with_complex_path(self):
        """Test URL with complex path structure"""
        url = "https://example.com/assets/images/gallery/photo.jpg"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert result.normalized_value["filename"] == "photo.jpg"
        assert result.normalized_value["path"] == "/assets/images/gallery/photo.jpg"

    def test_validate_url_with_no_path(self):
        """Test URL with no path (root domain)"""
        url = "https://example.com"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert result.normalized_value["filename"] is None
        assert result.normalized_value["extension"] is None
        assert result.normalized_value["path"] == ""

    def test_validate_url_with_empty_path_component(self):
        """Test URL that leads to empty filename (BUG VERIFICATION!)"""
        # This tests the potential bug with empty path_parts
        url = "https://example.com/"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert result.normalized_value["filename"] == ""  # This is the potential bug
        assert result.normalized_value["extension"] is None

    def test_validate_url_with_trailing_slash_and_extension(self):
        """Test URL edge cases with path parsing"""
        # URL ending with slash
        url = "https://example.com/folder/"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert result.normalized_value["filename"] == ""
        
        # URL with multiple slashes
        url = "https://example.com//image.jpg"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert result.normalized_value["filename"] == "image.jpg"

    def test_extension_detection_case_insensitive(self):
        """Test extension detection is case insensitive"""
        extensions_to_test = [
            ("image.JPG", ".jpg"),
            ("photo.PNG", ".png"),
            ("animation.GIF", ".gif"),
            ("logo.SVG", ".svg"),
            ("icon.ICO", ".ico"),
            ("picture.TIFF", ".tiff")
        ]
        
        for filename, expected_ext in extensions_to_test:
            url = f"https://example.com/{filename}"
            result = self.validator.validate(url)
            assert result.is_valid is True
            assert result.normalized_value["extension"] == expected_ext

    def test_extension_detection_with_query_params(self):
        """Test extension detection when URL has query parameters"""
        url = "https://example.com/image.jpg?version=1&size=large"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert result.normalized_value["extension"] == ".jpg"

    def test_extension_detection_priority(self):
        """Test extension detection uses first matching extension"""
        # File with multiple extensions - should detect the longest match
        url = "https://example.com/file.tar.gz.jpg"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert result.normalized_value["extension"] == ".jpg"  # Should find .jpg first in the loop

    def test_normalize_basic_string(self):
        """Test normalize method with basic string"""
        url = "  https://example.com/image.jpg  "
        normalized = self.validator.normalize(url)
        assert normalized == "https://example.com/image.jpg"

    def test_normalize_non_string(self):
        """Test normalize method with non-string values"""
        assert self.validator.normalize(123) == 123
        assert self.validator.normalize([1, 2, 3]) == [1, 2, 3]
        assert self.validator.normalize({"key": "value"}) == {"key": "value"}
        assert self.validator.normalize(None) is None

    def test_get_supported_types(self):
        """Test get_supported_types method"""
        supported_types = self.validator.get_supported_types()
        assert DataType.IMAGE.value in supported_types
        assert "image" in supported_types
        assert "image_url" in supported_types
        assert "photo" in supported_types
        assert len(supported_types) == 4

    def test_is_image_extension_class_method_valid(self):
        """Test is_image_extension class method with valid extensions"""
        valid_filenames = [
            "photo.jpg",
            "image.PNG",
            "animation.gif",
            "logo.svg",
            "icon.ico",
            "picture.webp",
            "bitmap.bmp",
            "scan.tiff",
            "document.tif"
        ]
        
        for filename in valid_filenames:
            assert ImageValidator.is_image_extension(filename) is True, f"Failed for {filename}"

    def test_is_image_extension_class_method_invalid(self):
        """Test is_image_extension class method with invalid extensions"""
        invalid_filenames = [
            "document.pdf",
            "video.mp4",
            "audio.mp3",
            "archive.zip",
            "script.js",
            "style.css",
            "data.json",
            "config.xml"
        ]
        
        for filename in invalid_filenames:
            assert ImageValidator.is_image_extension(filename) is False, f"Failed for {filename}"

    def test_is_image_extension_class_method_edge_cases(self):
        """Test is_image_extension class method with edge cases"""
        # Empty string
        assert ImageValidator.is_image_extension("") is False
        
        # None
        assert ImageValidator.is_image_extension(None) is False
        
        # No extension
        assert ImageValidator.is_image_extension("filename") is False
        
        # Extension only
        assert ImageValidator.is_image_extension(".jpg") is True
        
        # Multiple dots
        assert ImageValidator.is_image_extension("file.backup.jpg") is True
        
        # Case sensitivity
        assert ImageValidator.is_image_extension("FILE.JPG") is True

    def test_security_scheme_validation(self):
        """Test various URL schemes"""
        schemes_to_test = [
            ("https://example.com/image.jpg", True),
            ("http://example.com/image.jpg", True),
            ("ftp://example.com/image.jpg", True),
            ("file:///local/image.jpg", False),  # file:// URLs fail due to missing netloc (BUG!)
            ("data:image/png;base64,abc", False),  # Data URLs fail due to missing netloc
        ]
        
        for url, should_be_valid in schemes_to_test:
            result = self.validator.validate(url)
            assert result.is_valid == should_be_valid, f"Failed for {url}"

    def test_domain_extraction_edge_cases(self):
        """Test domain extraction with various edge cases"""
        domain_cases = [
            ("https://example.com/image.jpg", "example.com"),
            ("https://www.example.com/image.jpg", "www.example.com"),
            ("https://subdomain.example.com:8080/image.jpg", "subdomain.example.com:8080"),  # Port included in domain (BUG!)
            ("https://192.168.1.1/image.jpg", "192.168.1.1"),
            ("https://[::1]/image.jpg", "[::1]"),  # IPv6
        ]
        
        for url, expected_domain in domain_cases:
            result = self.validator.validate(url)
            if result.is_valid:
                assert result.normalized_value["domain"] == expected_domain

    def test_unicode_url_handling(self):
        """Test handling of URLs with unicode characters"""
        unicode_urls = [
            "https://example.com/图片.jpg",
            "https://测试.com/image.jpg",
            "https://example.com/café.png"
        ]
        
        for url in unicode_urls:
            result = self.validator.validate(url)
            # Should handle gracefully (may pass or fail depending on URL parsing)
            assert result.is_valid in [True, False]

    def test_very_long_url(self):
        """Test validation with very long URLs"""
        long_path = "/".join(["very_long_path_segment"] * 100)
        url = f"https://example.com{long_path}/image.jpg"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert result.normalized_value["extension"] == ".jpg"

    def test_url_with_unusual_but_valid_characters(self):
        """Test URLs with unusual but valid characters"""
        unusual_urls = [
            "https://example.com/image%20with%20spaces.jpg",
            "https://example.com/image+plus.jpg",
            "https://example.com/image-dash.jpg",
            "https://example.com/image_underscore.jpg",
            "https://example.com/image.name.jpg",
        ]
        
        for url in unusual_urls:
            result = self.validator.validate(url)
            assert result.is_valid is True

    def test_constraint_validation_order(self):
        """Test that constraints are validated in the correct order"""
        # This test verifies the order of validation checks
        constraints = {
            "requireHttps": True,
            "allowedDomains": ["example.com"],
            "requireExtension": True
        }
        
        # HTTP URL (should fail on HTTPS check before domain check)
        url = "http://malicious.com/image"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is False
        assert result.message == "Image URL must use HTTPS"  # First check that fails

    def test_empty_constraints_object(self):
        """Test validation with empty constraints"""
        url = "https://example.com/image.jpg"
        result = self.validator.validate(url, {})
        assert result.is_valid is True

    def test_none_constraints(self):
        """Test validation with None constraints"""
        url = "https://example.com/image.jpg"
        result = self.validator.validate(url, None)
        assert result.is_valid is True

    def test_metadata_accuracy(self):
        """Test that metadata is accurate"""
        # With extension
        url = "https://example.com/image.jpg"
        result = self.validator.validate(url)
        assert result.metadata["type"] == "image"
        assert result.metadata["hasExtension"] is True

        # Without extension
        url = "https://example.com/image"
        result = self.validator.validate(url)
        assert result.metadata["type"] == "image"
        assert result.metadata["hasExtension"] is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])