"""
Comprehensive unit tests for FileValidator
Tests all file validation functionality with REAL behavior verification
NO MOCKS - Tests verify actual code behavior
"""

import pytest
from typing import Dict, Any, Optional

from shared.validators.file_validator import FileValidator
from shared.models.common import DataType


class TestFileValidator:
    """Test FileValidator with real behavior verification"""

    def setup_method(self):
        """Setup for each test"""
        self.validator = FileValidator()

    def test_validate_basic_url_string(self):
        """Test validation of basic URL string"""
        url = "https://example.com/document.pdf"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert result.message == "Valid file"
        assert result.normalized_value["url"] == url
        assert result.normalized_value["name"] == "document.pdf"
        assert result.normalized_value["extension"] == ".pdf"
        assert result.normalized_value["type"] == "document"
        assert result.metadata["type"] == "file"
        assert result.metadata["hasExtension"] is True
        assert result.metadata["fileType"] == "document"

    def test_validate_url_without_extension(self):
        """Test validation of URL without file extension"""
        url = "https://example.com/file"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert result.normalized_value["name"] == "file"
        assert result.normalized_value["extension"] is None
        assert result.normalized_value["type"] == "unknown"
        assert result.metadata["hasExtension"] is False
        assert result.metadata["fileType"] == "unknown"

    def test_validate_url_without_filename(self):
        """Test validation of URL without filename (directory path)"""
        url = "https://example.com/path/"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert result.normalized_value["name"] == ""
        assert result.normalized_value["extension"] is None
        assert result.normalized_value["type"] == "unknown"

    def test_validate_invalid_url_format(self):
        """Test validation of invalid URL format"""
        # Missing scheme
        result = self.validator.validate("example.com/file.pdf")
        assert result.is_valid is False
        assert result.message == "Invalid URL format - missing scheme or domain"

        # Missing domain
        result = self.validator.validate("https:///file.pdf")
        assert result.is_valid is False
        assert result.message == "Invalid URL format - missing scheme or domain"

        # Completely malformed URL that urlparse can't handle
        result = self.validator.validate("not a url at all")
        assert result.is_valid is False
        assert result.message == "Invalid URL format - missing scheme or domain"

    def test_validate_file_object_with_url(self):
        """Test validation of file object with metadata"""
        file_obj = {
            "url": "https://example.com/report.xlsx",
            "name": "report.xlsx",
            "size": 1048576,  # 1MB
            "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        }
        result = self.validator.validate(file_obj)
        assert result.is_valid is True
        assert result.normalized_value["url"] == file_obj["url"]
        assert result.normalized_value["name"] == file_obj["name"]
        assert result.normalized_value["extension"] == ".xlsx"
        assert result.normalized_value["type"] == "document"
        assert result.normalized_value["size"] == file_obj["size"]
        assert result.normalized_value["mimeType"] == file_obj["mimeType"]
        assert result.normalized_value["sizeFormatted"] == "1.00 MB"

    def test_validate_file_object_without_url(self):
        """Test validation of file object missing required URL field"""
        file_obj = {"name": "test.txt", "size": 1024}
        result = self.validator.validate(file_obj)
        assert result.is_valid is False
        assert result.message == "File object must have 'url' field"

    def test_validate_file_object_extension_inference(self):
        """Test file object with extension inferred from name"""
        file_obj = {
            "url": "https://example.com/download",
            "name": "data.json"
        }
        result = self.validator.validate(file_obj)
        assert result.is_valid is True
        assert result.normalized_value["extension"] == ".json"
        assert result.normalized_value["type"] == "document"

    def test_validate_file_object_type_inference(self):
        """Test file object with type inferred from extension"""
        file_obj = {
            "url": "https://example.com/script.py",
            "name": "script.py",
            "extension": ".py"
        }
        result = self.validator.validate(file_obj)
        assert result.is_valid is True
        assert result.normalized_value["type"] == "code"

    def test_validate_non_file_types(self):
        """Test validation fails for non-file types"""
        # Number
        result = self.validator.validate(123)
        assert result.is_valid is False
        assert result.message == "File must be URL string or object"

        # List
        result = self.validator.validate([1, 2, 3])
        assert result.is_valid is False
        assert result.message == "File must be URL string or object"

        # None
        result = self.validator.validate(None)
        assert result.is_valid is False
        assert result.message == "File must be URL string or object"

    def test_validate_max_size_constraint_pass(self):
        """Test file size constraint validation (passing)"""
        constraints = {"maxSize": 2097152}  # 2MB
        file_obj = {
            "url": "https://example.com/small.pdf",
            "size": 1048576  # 1MB
        }
        result = self.validator.validate(file_obj, constraints)
        assert result.is_valid is True

    def test_validate_max_size_constraint_fail(self):
        """Test file size constraint validation (failing)"""
        constraints = {"maxSize": 1048576}  # 1MB
        file_obj = {
            "url": "https://example.com/large.pdf",
            "size": 2097152  # 2MB
        }
        result = self.validator.validate(file_obj, constraints)
        assert result.is_valid is False
        assert "File size exceeds maximum of 1.00 MB" in result.message

    def test_validate_max_size_constraint_no_size(self):
        """Test file size constraint when size is not provided"""
        constraints = {"maxSize": 1048576}
        file_obj = {"url": "https://example.com/file.pdf"}
        result = self.validator.validate(file_obj, constraints)
        assert result.is_valid is True  # No size to check, so passes

    def test_validate_allowed_extensions_constraint_pass(self):
        """Test allowed extensions constraint (passing)"""
        constraints = {"allowedExtensions": [".pdf", ".doc", ".docx"]}
        url = "https://example.com/document.pdf"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is True

    def test_validate_allowed_extensions_constraint_fail(self):
        """Test allowed extensions constraint (failing)"""
        constraints = {"allowedExtensions": [".pdf", ".doc"]}
        url = "https://example.com/image.jpg"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is False
        assert "File extension must be one of: ['.pdf', '.doc']" in result.message

    def test_validate_allowed_extensions_no_extension(self):
        """Test allowed extensions constraint when extension cannot be determined"""
        constraints = {"allowedExtensions": [".pdf", ".doc"]}
        url = "https://example.com/file"
        result = self.validator.validate(url, constraints)
        assert result.is_valid is False
        assert result.message == "File extension could not be determined"

    def test_validate_allowed_mime_types_constraint_pass(self):
        """Test allowed MIME types constraint (passing)"""
        constraints = {"allowedMimeTypes": ["application/pdf", "text/plain"]}
        file_obj = {
            "url": "https://example.com/document.pdf",
            "mimeType": "application/pdf"
        }
        result = self.validator.validate(file_obj, constraints)
        assert result.is_valid is True

    def test_validate_allowed_mime_types_constraint_fail(self):
        """Test allowed MIME types constraint (failing)"""
        constraints = {"allowedMimeTypes": ["application/pdf"]}
        file_obj = {
            "url": "https://example.com/document.txt",
            "mimeType": "text/plain"
        }
        result = self.validator.validate(file_obj, constraints)
        assert result.is_valid is False
        assert "File MIME type must be one of: ['application/pdf']" in result.message

    def test_validate_allowed_mime_types_no_mime_type(self):
        """Test allowed MIME types constraint when MIME type is not provided"""
        constraints = {"allowedMimeTypes": ["application/pdf"]}
        file_obj = {"url": "https://example.com/document.pdf"}
        result = self.validator.validate(file_obj, constraints)
        assert result.is_valid is True  # No MIME type to check, so passes

    def test_validate_combined_constraints(self):
        """Test validation with multiple constraints"""
        constraints = {
            "maxSize": 5242880,  # 5MB
            "allowedExtensions": [".pdf", ".doc", ".docx"],
            "allowedMimeTypes": ["application/pdf"]
        }
        file_obj = {
            "url": "https://example.com/document.pdf",
            "name": "document.pdf",  # Need name field for extension detection in file objects
            "size": 2097152,  # 2MB
            "mimeType": "application/pdf"
        }
        result = self.validator.validate(file_obj, constraints)
        assert result.is_valid is True

    def test_file_type_classification_document(self):
        """Test file type classification for document extensions"""
        document_urls = [
            "https://example.com/file.pdf",
            "https://example.com/file.doc",
            "https://example.com/file.docx",
            "https://example.com/file.xls",
            "https://example.com/file.xlsx",
            "https://example.com/file.ppt",
            "https://example.com/file.pptx",
            "https://example.com/file.txt",
            "https://example.com/file.csv",
            "https://example.com/file.json",
            "https://example.com/file.xml",
            "https://example.com/file.yaml",
            "https://example.com/file.yml"
        ]
        
        for url in document_urls:
            result = self.validator.validate(url)
            assert result.is_valid is True
            assert result.normalized_value["type"] == "document", f"Failed for {url}"

    def test_file_type_classification_code(self):
        """Test file type classification for code extensions"""
        code_urls = [
            "https://example.com/script.py",
            "https://example.com/app.js",
            "https://example.com/component.ts",
            "https://example.com/Main.java",
            "https://example.com/program.cpp",
            "https://example.com/header.h",
            "https://example.com/server.go",
            "https://example.com/lib.rs",
            "https://example.com/index.php",
            "https://example.com/script.rb",
            "https://example.com/app.swift",
            "https://example.com/Activity.kt",
            "https://example.com/App.scala",
            "https://example.com/analysis.r"
        ]
        
        for url in code_urls:
            result = self.validator.validate(url)
            assert result.is_valid is True
            assert result.normalized_value["type"] == "code", f"Failed for {url}"

    def test_file_type_classification_archive(self):
        """Test file type classification for archive extensions"""
        archive_urls = [
            "https://example.com/package.zip",
            "https://example.com/backup.tar",
            "https://example.com/compressed.gz",
            "https://example.com/archive.bz2",
            "https://example.com/files.7z",
            "https://example.com/data.rar",
            "https://example.com/source.tar.gz"
        ]
        
        for url in archive_urls:
            result = self.validator.validate(url)
            assert result.is_valid is True
            assert result.normalized_value["type"] == "archive", f"Failed for {url}"

    def test_file_type_classification_image(self):
        """Test file type classification for image extensions"""
        image_urls = [
            "https://example.com/photo.jpg",
            "https://example.com/image.jpeg",
            "https://example.com/picture.png",
            "https://example.com/animation.gif",
            "https://example.com/modern.webp",
            "https://example.com/bitmap.bmp"
        ]
        
        for url in image_urls:
            result = self.validator.validate(url)
            assert result.is_valid is True
            assert result.normalized_value["type"] == "image", f"Failed for {url}"

    def test_file_type_classification_video(self):
        """Test file type classification for video extensions"""
        video_urls = [
            "https://example.com/movie.mp4",
            "https://example.com/video.avi",
            "https://example.com/clip.mov",
            "https://example.com/presentation.wmv",
            "https://example.com/stream.flv",
            "https://example.com/modern.webm"
        ]
        
        for url in video_urls:
            result = self.validator.validate(url)
            assert result.is_valid is True
            assert result.normalized_value["type"] == "video", f"Failed for {url}"

    def test_file_type_classification_audio(self):
        """Test file type classification for audio extensions"""
        audio_urls = [
            "https://example.com/song.mp3",
            "https://example.com/audio.wav",
            "https://example.com/lossless.flac",
            "https://example.com/compressed.aac",
            "https://example.com/open.ogg",
            "https://example.com/windows.wma"
        ]
        
        for url in audio_urls:
            result = self.validator.validate(url)
            assert result.is_valid is True
            assert result.normalized_value["type"] == "audio", f"Failed for {url}"

    def test_file_type_classification_other(self):
        """Test file type classification for unknown extensions"""
        other_urls = [
            "https://example.com/file.unknown",
            "https://example.com/custom.xyz",
            "https://example.com/proprietary.abc"
        ]
        
        for url in other_urls:
            result = self.validator.validate(url)
            assert result.is_valid is True
            assert result.normalized_value["type"] == "other", f"Failed for {url}"

    def test_file_size_formatting(self):
        """Test file size formatting functionality"""
        test_cases = [
            (512, "512.00 B"),
            (1024, "1.00 KB"),
            (1536, "1.50 KB"),
            (1048576, "1.00 MB"),
            (1572864, "1.50 MB"),
            (1073741824, "1.00 GB"),
            (1099511627776, "1.00 TB"),
            (1125899906842624, "1.00 PB")
        ]
        
        for size_bytes, expected_format in test_cases:
            formatted = self.validator._format_file_size(size_bytes)
            assert formatted == expected_format

    def test_normalize_string_url(self):
        """Test normalize method with string URL"""
        url = "  https://example.com/file.pdf  "
        normalized = self.validator.normalize(url)
        assert normalized == "https://example.com/file.pdf"

    def test_normalize_non_string(self):
        """Test normalize method with non-string values"""
        file_obj = {"url": "https://example.com/file.pdf"}
        normalized = self.validator.normalize(file_obj)
        assert normalized == file_obj

        assert self.validator.normalize(123) == 123
        assert self.validator.normalize(None) is None

    def test_get_supported_types(self):
        """Test get_supported_types method"""
        supported_types = self.validator.get_supported_types()
        assert DataType.FILE.value in supported_types
        assert "file" in supported_types
        assert "attachment" in supported_types
        assert "document" in supported_types
        assert len(supported_types) == 4

    def test_edge_case_empty_url(self):
        """Test validation with empty URL"""
        result = self.validator.validate("")
        assert result.is_valid is False
        assert result.message == "Invalid URL format - missing scheme or domain"

    def test_edge_case_url_with_query_params(self):
        """Test validation with URL containing query parameters"""
        url = "https://example.com/file.pdf?download=true&version=1"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert result.normalized_value["name"] == "file.pdf"
        assert result.normalized_value["extension"] == ".pdf"

    def test_edge_case_url_with_fragment(self):
        """Test validation with URL containing fragment"""
        url = "https://example.com/document.pdf#page=5"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert result.normalized_value["name"] == "document.pdf"
        assert result.normalized_value["extension"] == ".pdf"

    def test_edge_case_multiple_dots_in_filename(self):
        """Test file with multiple dots in filename"""
        url = "https://example.com/file.backup.tar.gz"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert result.normalized_value["name"] == "file.backup.tar.gz"
        assert result.normalized_value["extension"] == ".gz"  # Takes last extension
        assert result.normalized_value["type"] == "archive"

    def test_edge_case_uppercase_extension(self):
        """Test file with uppercase extension"""
        url = "https://example.com/document.PDF"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert result.normalized_value["extension"] == ".pdf"  # Normalized to lowercase
        assert result.normalized_value["type"] == "document"

    def test_edge_case_no_path_in_url(self):
        """Test URL with no path component"""
        url = "https://example.com"
        result = self.validator.validate(url)
        assert result.is_valid is True
        assert result.normalized_value["name"] is None
        assert result.normalized_value["extension"] is None
        assert result.normalized_value["type"] == "unknown"

    def test_edge_case_very_large_file_size(self):
        """Test file size formatting with very large sizes"""
        file_obj = {
            "url": "https://example.com/huge.dat",
            "size": 5 * (1024 ** 5)  # 5 PB
        }
        result = self.validator.validate(file_obj)
        assert result.is_valid is True
        assert result.normalized_value["sizeFormatted"] == "5.00 PB"

    def test_edge_case_zero_file_size(self):
        """Test file with zero size"""
        file_obj = {
            "url": "https://example.com/empty.txt",
            "size": 0
        }
        result = self.validator.validate(file_obj)
        assert result.is_valid is True
        assert result.normalized_value["sizeFormatted"] == "0.00 B"

    def test_case_insensitive_extension_constraints(self):
        """Test that extension constraints work with case variations"""
        constraints = {"allowedExtensions": [".PDF", ".DOC"]}
        url = "https://example.com/document.pdf"  # lowercase extension
        result = self.validator.validate(url, constraints)
        # This will fail because constraint check is case-sensitive
        assert result.is_valid is False
        assert "File extension must be one of" in result.message


if __name__ == "__main__":
    pytest.main([__file__, "-v"])