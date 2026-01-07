"""
GeoPoint validator for SPICE HARVESTER.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from .base_validator import BaseValidator, ValidationResult


_GEOHASH_RE = re.compile(r"^[0123456789bcdefghjkmnpqrstuvwxyz]+$")


class GeoPointValidator(BaseValidator):
    """Validator for geopoint values (lat,lon or geohash)."""

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        if constraints is None:
            constraints = {}

        normalized = None
        fmt = str(constraints.get("format") or "").strip().lower()

        if isinstance(value, dict):
            lat = value.get("lat") or value.get("latitude")
            lon = value.get("lon") or value.get("lng") or value.get("longitude")
            try:
                lat_f = float(lat)
                lon_f = float(lon)
            except Exception:
                return ValidationResult(is_valid=False, message="Invalid geopoint dictionary")
            if not (-90.0 <= lat_f <= 90.0 and -180.0 <= lon_f <= 180.0):
                return ValidationResult(is_valid=False, message="Geopoint out of range")
            normalized = f"{lat_f},{lon_f}"
        elif isinstance(value, str):
            s = value.strip()
            if not s:
                return ValidationResult(is_valid=False, message="Geopoint must be non-empty")
            if "," in s:
                if fmt and fmt not in {"latlon", "lat,lng", "lat,lon"}:
                    return ValidationResult(is_valid=False, message="Geopoint must be a geohash")
                try:
                    lat_str, lon_str = [part.strip() for part in s.split(",", 1)]
                    lat_f = float(lat_str)
                    lon_f = float(lon_str)
                except Exception:
                    return ValidationResult(is_valid=False, message="Invalid geopoint lat/lon format")
                if not (-90.0 <= lat_f <= 90.0 and -180.0 <= lon_f <= 180.0):
                    return ValidationResult(is_valid=False, message="Geopoint out of range")
                normalized = f"{lat_f},{lon_f}"
            else:
                if fmt and fmt not in {"geohash"}:
                    return ValidationResult(is_valid=False, message="Geopoint must be lat,lon format")
                if not _GEOHASH_RE.match(s):
                    return ValidationResult(is_valid=False, message="Invalid geohash format")
                normalized = s
        else:
            return ValidationResult(is_valid=False, message="Geopoint must be string or object")

        return ValidationResult(
            is_valid=True,
            message="Geopoint validation passed",
            normalized_value=normalized,
        )

    def normalize(self, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip()
        return value

    def get_supported_types(self) -> List[str]:
        return ["geopoint"]
