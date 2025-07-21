"""
JSON-LD utilities for SPICE HARVESTER
"""

import json
from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class JSONToJSONLDConverter:
    """
    Converter for transforming JSON data to JSON-LD format.
    """

    def __init__(self, context: Optional[Dict[str, Any]] = None):
        """
        Initialize converter with optional context.

        Args:
            context: JSON-LD context dictionary
        """
        self.context = context or self._get_default_context()

    def _get_default_context(self) -> Dict[str, Any]:
        """Get default JSON-LD context."""
        return {
            "@context": {
                "@vocab": "http://spice-harvester.org/ontology#",
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "owl": "http://www.w3.org/2002/07/owl#",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
                "label": "rdfs:label",
                "comment": "rdfs:comment",
                "type": "@type",
                "id": "@id",
            }
        }

    def convert_to_jsonld(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert JSON data to JSON-LD format.

        Args:
            data: Input JSON data

        Returns:
            JSON-LD formatted data
        """
        # Add context if not present
        if "@context" not in data:
            data["@context"] = self.context.get("@context", {})

        # Add type if not present
        if "@type" not in data and "type" not in data:
            data["@type"] = "owl:Class"

        # Convert id field to @id
        if "id" in data and "@id" not in data:
            data["@id"] = data["id"]
            del data["id"]

        # Convert label and description
        if "label" in data:
            data["rdfs:label"] = data["label"]
            del data["label"]

        if "description" in data:
            data["rdfs:comment"] = data["description"]
            del data["description"]

        return data

    def convert_from_jsonld(self, jsonld_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert JSON-LD data back to regular JSON format.

        Args:
            jsonld_data: Input JSON-LD data

        Returns:
            Regular JSON data
        """
        data = dict(jsonld_data)

        # Remove context
        if "@context" in data:
            del data["@context"]

        # Convert @id back to id
        if "@id" in data:
            data["id"] = data["@id"]
            del data["@id"]

        # Convert @type back to type
        if "@type" in data:
            data["type"] = data["@type"]
            del data["@type"]

        # Convert rdfs:label back to label
        if "rdfs:label" in data:
            data["label"] = data["rdfs:label"]
            del data["rdfs:label"]

        # Convert rdfs:comment back to description
        if "rdfs:comment" in data:
            data["description"] = data["rdfs:comment"]
            del data["rdfs:comment"]

        return data

    def validate_jsonld(self, data: Dict[str, Any]) -> bool:
        """
        Validate if data is valid JSON-LD.

        Args:
            data: Data to validate

        Returns:
            True if valid JSON-LD, False otherwise
        """
        try:
            # Check for required JSON-LD fields
            if not isinstance(data, dict):
                return False

            # Must have context or be in a context
            if "@context" not in data and not hasattr(self, "_global_context"):
                return False

            # Should have @id or @type
            if "@id" not in data and "@type" not in data:
                return False

            return True
        except Exception as e:
            logger.debug(f"Failed to validate JSON-LD data: {e}")
            return False

    def expand_jsonld(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Expand JSON-LD data by resolving context.

        Args:
            data: JSON-LD data to expand

        Returns:
            Expanded JSON-LD data
        """
        # Simple expansion - in production, use a proper JSON-LD processor
        expanded = dict(data)

        context = data.get("@context", self.context.get("@context", {}))

        # Expand prefixed properties
        for key, value in list(expanded.items()):
            if ":" in key and key != "@context":
                prefix, local = key.split(":", 1)
                if prefix in context:
                    expanded_key = context[prefix] + local
                    expanded[expanded_key] = value
                    del expanded[key]

        return expanded

    def compact_jsonld(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compact JSON-LD data using context.

        Args:
            data: JSON-LD data to compact

        Returns:
            Compacted JSON-LD data
        """
        # Simple compaction - in production, use a proper JSON-LD processor
        compacted = dict(data)

        context = self.context.get("@context", {})

        # Create reverse mapping
        reverse_context = {}
        for prefix, uri in context.items():
            if isinstance(uri, str) and uri.startswith("http"):
                reverse_context[uri] = prefix

        # Compact URIs to prefixes
        for key, value in list(compacted.items()):
            if key.startswith("http"):
                for uri, prefix in reverse_context.items():
                    if key.startswith(uri):
                        local = key[len(uri) :]
                        compacted_key = f"{prefix}:{local}"
                        compacted[compacted_key] = value
                        del compacted[key]
                        break

        return compacted

    def to_json_string(self, data: Dict[str, Any], indent: int = 2) -> str:
        """
        Convert data to JSON-LD string.

        Args:
            data: Data to convert
            indent: JSON indentation

        Returns:
            JSON-LD string
        """
        jsonld_data = self.convert_to_jsonld(data)
        return json.dumps(jsonld_data, indent=indent)

    def from_json_string(self, json_string: str) -> Dict[str, Any]:
        """
        Parse JSON-LD string to data.

        Args:
            json_string: JSON-LD string

        Returns:
            Parsed data
        """
        data = json.loads(json_string)
        return self.convert_from_jsonld(data)


def get_default_converter() -> JSONToJSONLDConverter:
    """Get default JSON-LD converter."""
    return JSONToJSONLDConverter()


if __name__ == "__main__":
    # Test the converter
    converter = JSONToJSONLDConverter()

    # Test data
    test_data = {"id": "Person", "label": "Person", "description": "A human being", "type": "Class"}

    print("Original data:", test_data)

    # Convert to JSON-LD
    jsonld_data = converter.convert_to_jsonld(test_data.copy())
    print("JSON-LD data:", jsonld_data)

    # Convert back
    regular_data = converter.convert_from_jsonld(jsonld_data)
    print("Regular data:", regular_data)

    # Validate
    print("Is valid JSON-LD:", converter.validate_jsonld(jsonld_data))
