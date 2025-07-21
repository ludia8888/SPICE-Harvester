"""
IP address validator for SPICE HARVESTER
"""

import ipaddress
import re
from typing import Any, Dict, List, Optional
import logging

from ..models.common import DataType
from .base_validator import BaseValidator, ValidationResult

logger = logging.getLogger(__name__)


class IpValidator(BaseValidator):
    """Validator for IP addresses"""

    # IPv4 pattern
    IPV4_PATTERN = r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"

    # IPv6 pattern (simplified)
    IPV6_PATTERN = r"^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|::1|::)$"

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate IP address"""
        if constraints is None:
            constraints = {}

        # Type check
        if not isinstance(value, str):
            return ValidationResult(
                is_valid=False, message=f"Expected string, got {type(value).__name__}"
            )

        # Version constraint
        ip_version = constraints.get("version", "any")  # "4", "6", or "any"

        # Try to parse as IP address
        try:
            ip_obj = ipaddress.ip_address(value)

            # Check version constraint
            if ip_version == "4" and ip_obj.version != 4:
                return ValidationResult(is_valid=False, message="Expected IPv4 address")
            elif ip_version == "6" and ip_obj.version != 6:
                return ValidationResult(is_valid=False, message="Expected IPv6 address")

            # Check if private/public constraint
            if "requirePrivate" in constraints:
                if constraints["requirePrivate"] and not ip_obj.is_private:
                    return ValidationResult(is_valid=False, message="IP address must be private")
                elif not constraints["requirePrivate"] and ip_obj.is_private:
                    return ValidationResult(is_valid=False, message="IP address must be public")

            # Check if loopback constraint
            if constraints.get("allowLoopback", True) is False and ip_obj.is_loopback:
                return ValidationResult(
                    is_valid=False, message="Loopback addresses are not allowed"
                )

            # Check if multicast constraint
            if constraints.get("allowMulticast", True) is False and ip_obj.is_multicast:
                return ValidationResult(
                    is_valid=False, message="Multicast addresses are not allowed"
                )

            # Check allowed ranges
            if "allowedRanges" in constraints:
                allowed = False
                for range_str in constraints["allowedRanges"]:
                    try:
                        network = ipaddress.ip_network(range_str, strict=False)
                        if ip_obj in network:
                            allowed = True
                            break
                    except (ValueError, ipaddress.AddressValueError, ipaddress.NetmaskValueError):
                        continue

                if not allowed:
                    return ValidationResult(
                        is_valid=False,
                        message=f"IP address must be within allowed ranges: {constraints['allowedRanges']}",
                    )

            # Check blocked ranges
            if "blockedRanges" in constraints:
                for range_str in constraints["blockedRanges"]:
                    try:
                        network = ipaddress.ip_network(range_str, strict=False)
                        if ip_obj in network:
                            return ValidationResult(
                                is_valid=False,
                                message=f"IP address is in blocked range: {range_str}",
                            )
                    except (ValueError, ipaddress.AddressValueError, ipaddress.NetmaskValueError):
                        continue

            result = {
                "address": str(ip_obj),
                "version": ip_obj.version,
                "isPrivate": ip_obj.is_private,
                "isLoopback": ip_obj.is_loopback,
                "isMulticast": ip_obj.is_multicast,
                "isReserved": ip_obj.is_reserved,
            }

            return ValidationResult(
                is_valid=True,
                message="IP validation passed",
                normalized_value=result,
                metadata={"type": "ip", "version": ip_obj.version},
            )

        except ValueError:
            return ValidationResult(is_valid=False, message="Invalid IP address format")

    def normalize(self, value: Any) -> Any:
        """Normalize IP address"""
        if not isinstance(value, str):
            return value

        # Strip whitespace
        value = value.strip()

        # Try to parse and return canonical form
        try:
            ip_obj = ipaddress.ip_address(value)
            return str(ip_obj)
        except (ValueError, ipaddress.AddressValueError) as e:
            logger.debug(f"Failed to normalize IP address '{value}': {e}")
            return value

    def get_supported_types(self) -> List[str]:
        """Get supported types"""
        return ["ip", "ipaddress", "ip_address", "ipv4", "ipv6"]
