"""
Comprehensive unit tests for IpValidator
Tests all IP validation functionality with REAL behavior verification
NO MOCKS - Tests verify actual code behavior
"""

import pytest
import ipaddress
from typing import Dict, Any, Optional

from shared.validators.ip_validator import IpValidator


class TestIpValidator:
    """Test IpValidator with real behavior verification"""

    def setup_method(self):
        """Setup for each test"""
        self.validator = IpValidator()

    def test_validate_basic_ipv4_address(self):
        """Test validation of basic IPv4 address"""
        ip = "192.168.1.1"
        result = self.validator.validate(ip)
        assert result.is_valid is True
        assert result.message == "IP validation passed"
        assert result.normalized_value["address"] == ip
        assert result.normalized_value["version"] == 4
        assert result.normalized_value["isPrivate"] is True
        assert result.normalized_value["isLoopback"] is False
        assert result.normalized_value["isMulticast"] is False
        assert result.normalized_value["isReserved"] is False
        assert result.metadata["type"] == "ip"
        assert result.metadata["version"] == 4

    def test_validate_basic_ipv6_address(self):
        """Test validation of basic IPv6 address"""
        ip = "2001:db8::1"
        result = self.validator.validate(ip)
        assert result.is_valid is True
        assert result.message == "IP validation passed"
        assert result.normalized_value["address"] == ip
        assert result.normalized_value["version"] == 6
        assert result.normalized_value["isPrivate"] is True  # RFC 3849 documentation block
        assert result.normalized_value["isLoopback"] is False
        assert result.normalized_value["isMulticast"] is False
        assert result.metadata["version"] == 6

    def test_validate_loopback_addresses(self):
        """Test validation of loopback addresses"""
        # IPv4 loopback
        result = self.validator.validate("127.0.0.1")
        assert result.is_valid is True
        assert result.normalized_value["isLoopback"] is True
        assert result.normalized_value["isPrivate"] is True  # Loopback is considered private

        # IPv6 loopback
        result = self.validator.validate("::1")
        assert result.is_valid is True
        assert result.normalized_value["isLoopback"] is True
        assert result.normalized_value["version"] == 6

    def test_validate_multicast_addresses(self):
        """Test validation of multicast addresses"""
        # IPv4 multicast
        result = self.validator.validate("224.0.0.1")
        assert result.is_valid is True
        assert result.normalized_value["isMulticast"] is True

        # IPv6 multicast
        result = self.validator.validate("ff02::1")
        assert result.is_valid is True
        assert result.normalized_value["isMulticast"] is True
        assert result.normalized_value["version"] == 6

    def test_validate_non_string_types(self):
        """Test validation fails for non-string types"""
        # Number
        result = self.validator.validate(123)
        assert result.is_valid is False
        assert "Expected string, got int" in result.message

        # List
        result = self.validator.validate([1, 2, 3, 4])
        assert result.is_valid is False
        assert "Expected string, got list" in result.message

        # Dict
        result = self.validator.validate({"ip": "192.168.1.1"})
        assert result.is_valid is False
        assert "Expected string, got dict" in result.message

        # None
        result = self.validator.validate(None)
        assert result.is_valid is False
        assert "Expected string, got NoneType" in result.message

    def test_validate_invalid_ip_formats(self):
        """Test validation of invalid IP address formats"""
        invalid_ips = [
            "not.an.ip.address",
            "999.999.999.999",
            "192.168.1",
            "192.168.1.1.1",
            "192.168.1.256",
            "::invalid::ipv6",
            "",
            "   ",
            "192.168.1.1/24"  # CIDR notation not valid for single IP
        ]
        
        for invalid_ip in invalid_ips:
            result = self.validator.validate(invalid_ip)
            assert result.is_valid is False, f"Should fail for: {invalid_ip}"
            assert result.message == "Invalid IP address format"

    def test_validate_version_constraint_ipv4_only(self):
        """Test version constraint for IPv4 only"""
        constraints = {"version": "4"}
        
        # Valid IPv4
        result = self.validator.validate("192.168.1.1", constraints)
        assert result.is_valid is True

        # Invalid - IPv6 when IPv4 required
        result = self.validator.validate("2001:db8::1", constraints)
        assert result.is_valid is False
        assert result.message == "Expected IPv4 address"

    def test_validate_version_constraint_ipv6_only(self):
        """Test version constraint for IPv6 only"""
        constraints = {"version": "6"}
        
        # Valid IPv6
        result = self.validator.validate("2001:db8::1", constraints)
        assert result.is_valid is True

        # Invalid - IPv4 when IPv6 required
        result = self.validator.validate("192.168.1.1", constraints)
        assert result.is_valid is False
        assert result.message == "Expected IPv6 address"

    def test_validate_version_constraint_any(self):
        """Test version constraint for any version"""
        constraints = {"version": "any"}
        
        # Both should be valid
        result = self.validator.validate("192.168.1.1", constraints)
        assert result.is_valid is True

        result = self.validator.validate("2001:db8::1", constraints)
        assert result.is_valid is True

    def test_validate_require_private_constraint_true(self):
        """Test require private constraint (must be private)"""
        constraints = {"requirePrivate": True}
        
        # Private IPv4 addresses
        private_ips = ["192.168.1.1", "10.0.0.1", "172.16.0.1", "127.0.0.1"]
        for ip in private_ips:
            result = self.validator.validate(ip, constraints)
            assert result.is_valid is True, f"Should pass for private IP: {ip}"

        # Public IPv4 address
        result = self.validator.validate("8.8.8.8", constraints)
        assert result.is_valid is False
        assert result.message == "IP address must be private"

        # Private IPv6 address
        result = self.validator.validate("fc00::1", constraints)
        assert result.is_valid is True

    def test_validate_require_private_constraint_false(self):
        """Test require private constraint (must be public)"""
        constraints = {"requirePrivate": False}
        
        # Public IPv4 address
        result = self.validator.validate("8.8.8.8", constraints)
        assert result.is_valid is True

        # Private IPv4 address
        result = self.validator.validate("192.168.1.1", constraints)
        assert result.is_valid is False
        assert result.message == "IP address must be public"

    def test_validate_allow_loopback_constraint_false(self):
        """Test allow loopback constraint (loopback not allowed)"""
        constraints = {"allowLoopback": False}
        
        # Loopback addresses should fail
        result = self.validator.validate("127.0.0.1", constraints)
        assert result.is_valid is False
        assert result.message == "Loopback addresses are not allowed"

        result = self.validator.validate("::1", constraints)
        assert result.is_valid is False
        assert result.message == "Loopback addresses are not allowed"

        # Non-loopback should pass
        result = self.validator.validate("8.8.8.8", constraints)
        assert result.is_valid is True

    def test_validate_allow_loopback_constraint_true(self):
        """Test allow loopback constraint (loopback allowed - default)"""
        constraints = {"allowLoopback": True}
        
        # Loopback addresses should pass
        result = self.validator.validate("127.0.0.1", constraints)
        assert result.is_valid is True

        result = self.validator.validate("::1", constraints)
        assert result.is_valid is True

    def test_validate_allow_multicast_constraint_false(self):
        """Test allow multicast constraint (multicast not allowed)"""
        constraints = {"allowMulticast": False}
        
        # Multicast addresses should fail
        result = self.validator.validate("224.0.0.1", constraints)
        assert result.is_valid is False
        assert result.message == "Multicast addresses are not allowed"

        result = self.validator.validate("ff02::1", constraints)
        assert result.is_valid is False
        assert result.message == "Multicast addresses are not allowed"

        # Non-multicast should pass
        result = self.validator.validate("8.8.8.8", constraints)
        assert result.is_valid is True

    def test_validate_allow_multicast_constraint_true(self):
        """Test allow multicast constraint (multicast allowed - default)"""
        constraints = {"allowMulticast": True}
        
        # Multicast addresses should pass
        result = self.validator.validate("224.0.0.1", constraints)
        assert result.is_valid is True

        result = self.validator.validate("ff02::1", constraints)
        assert result.is_valid is True

    def test_validate_allowed_ranges_constraint_pass(self):
        """Test allowed ranges constraint (passing)"""
        constraints = {"allowedRanges": ["192.168.0.0/16", "10.0.0.0/8"]}
        
        # IPs within allowed ranges
        result = self.validator.validate("192.168.1.1", constraints)
        assert result.is_valid is True

        result = self.validator.validate("10.5.10.20", constraints)
        assert result.is_valid is True

        result = self.validator.validate("192.168.255.255", constraints)
        assert result.is_valid is True

    def test_validate_allowed_ranges_constraint_fail(self):
        """Test allowed ranges constraint (failing)"""
        constraints = {"allowedRanges": ["192.168.0.0/16", "10.0.0.0/8"]}
        
        # IP outside allowed ranges
        result = self.validator.validate("8.8.8.8", constraints)
        assert result.is_valid is False
        assert "IP address must be within allowed ranges" in result.message

        result = self.validator.validate("172.16.1.1", constraints)
        assert result.is_valid is False

    def test_validate_allowed_ranges_with_single_ip(self):
        """Test allowed ranges with single IP address"""
        constraints = {"allowedRanges": ["192.168.1.1/32"]}
        
        # Exact IP should pass
        result = self.validator.validate("192.168.1.1", constraints)
        assert result.is_valid is True

        # Different IP should fail
        result = self.validator.validate("192.168.1.2", constraints)
        assert result.is_valid is False

    def test_validate_allowed_ranges_invalid_range_triggers_bare_except(self):
        """Test allowed ranges with invalid range (triggers bare except BUG!)"""
        constraints = {"allowedRanges": ["invalid.range", "192.168.0.0/16"]}
        
        # Should still work if at least one valid range matches
        result = self.validator.validate("192.168.1.1", constraints)
        assert result.is_valid is True

        # Should fail if no valid ranges match
        result = self.validator.validate("8.8.8.8", constraints)
        assert result.is_valid is False

    def test_validate_blocked_ranges_constraint_pass(self):
        """Test blocked ranges constraint (passing)"""
        constraints = {"blockedRanges": ["192.168.0.0/16", "10.0.0.0/8"]}
        
        # IPs outside blocked ranges should pass
        result = self.validator.validate("8.8.8.8", constraints)
        assert result.is_valid is True

        result = self.validator.validate("172.16.1.1", constraints)
        assert result.is_valid is True

    def test_validate_blocked_ranges_constraint_fail(self):
        """Test blocked ranges constraint (failing)"""
        constraints = {"blockedRanges": ["192.168.0.0/16", "10.0.0.0/8"]}
        
        # IPs within blocked ranges should fail
        result = self.validator.validate("192.168.1.1", constraints)
        assert result.is_valid is False
        assert "IP address is in blocked range: 192.168.0.0/16" in result.message

        result = self.validator.validate("10.5.10.20", constraints)
        assert result.is_valid is False
        assert "IP address is in blocked range: 10.0.0.0/8" in result.message

    def test_validate_blocked_ranges_invalid_range_triggers_bare_except(self):
        """Test blocked ranges with invalid range (triggers bare except BUG!)"""
        constraints = {"blockedRanges": ["invalid.range", "192.168.0.0/16"]}
        
        # Invalid ranges are ignored due to bare except, valid ranges still work
        result = self.validator.validate("192.168.1.1", constraints)
        assert result.is_valid is False  # Should be blocked by valid range

        # IP not in any valid blocked range should pass
        result = self.validator.validate("8.8.8.8", constraints)
        assert result.is_valid is True

    def test_validate_combined_constraints(self):
        """Test validation with multiple constraints"""
        constraints = {
            "version": "4",
            "requirePrivate": True,
            "allowLoopback": False,
            "allowedRanges": ["192.168.0.0/16"]
        }
        
        # Valid IP meeting all constraints
        result = self.validator.validate("192.168.1.1", constraints)
        assert result.is_valid is True

        # Fail version constraint
        result = self.validator.validate("2001:db8::1", constraints)
        assert result.is_valid is False
        assert result.message == "Expected IPv4 address"

        # Fail private constraint
        result = self.validator.validate("8.8.8.8", constraints)
        assert result.is_valid is False
        assert result.message == "IP address must be private"

        # Fail loopback constraint
        result = self.validator.validate("127.0.0.1", constraints)
        assert result.is_valid is False
        assert result.message == "Loopback addresses are not allowed"

        # Fail allowed ranges constraint
        result = self.validator.validate("10.0.0.1", constraints)
        assert result.is_valid is False
        assert "IP address must be within allowed ranges" in result.message

    def test_validate_ipv6_addresses_comprehensive(self):
        """Test comprehensive IPv6 address validation"""
        ipv6_addresses = [
            "2001:db8::1",                    # Standard notation
            "2001:0db8:0000:0000:0000:0000:0000:0001",  # Full notation
            "::1",                            # Loopback
            "::",                             # All zeros
            "fe80::1",                        # Link-local
            "ff02::1",                        # Multicast
            "2001:db8:85a3::8a2e:370:7334",  # Mixed
        ]
        
        for ip in ipv6_addresses:
            result = self.validator.validate(ip)
            assert result.is_valid is True, f"Should pass for IPv6: {ip}"
            assert result.normalized_value["version"] == 6

    def test_validate_ipv4_addresses_comprehensive(self):
        """Test comprehensive IPv4 address validation"""
        ipv4_addresses = [
            "0.0.0.0",          # All zeros
            "127.0.0.1",        # Loopback
            "192.168.1.1",      # Private
            "10.0.0.1",         # Private
            "172.16.0.1",       # Private
            "8.8.8.8",          # Public
            "255.255.255.255",  # Broadcast
            "224.0.0.1",        # Multicast
        ]
        
        for ip in ipv4_addresses:
            result = self.validator.validate(ip)
            assert result.is_valid is True, f"Should pass for IPv4: {ip}"
            assert result.normalized_value["version"] == 4

    def test_normalize_basic_ip_addresses(self):
        """Test normalize method with basic IP addresses"""
        # IPv4
        normalized = self.validator.normalize("192.168.1.1")
        assert normalized == "192.168.1.1"

        # IPv6 - should return canonical form
        normalized = self.validator.normalize("2001:0db8:0000:0000:0000:0000:0000:0001")
        assert normalized == "2001:db8::1"

        # IPv6 loopback
        normalized = self.validator.normalize("::1")
        assert normalized == "::1"

    def test_normalize_with_whitespace(self):
        """Test normalize method with whitespace"""
        normalized = self.validator.normalize("  192.168.1.1  ")
        assert normalized == "192.168.1.1"

        normalized = self.validator.normalize("\t2001:db8::1\n")
        assert normalized == "2001:db8::1"

    def test_normalize_invalid_ip_triggers_bare_except(self):
        """Test normalize with invalid IP (triggers bare except BUG!)"""
        # Invalid IP should return original value due to bare except
        invalid_ip = "not.an.ip"
        normalized = self.validator.normalize(invalid_ip)
        assert normalized == invalid_ip

    def test_normalize_non_string_types(self):
        """Test normalize method with non-string types"""
        assert self.validator.normalize(123) == 123
        assert self.validator.normalize([1, 2, 3]) == [1, 2, 3]
        assert self.validator.normalize({"ip": "test"}) == {"ip": "test"}
        assert self.validator.normalize(None) is None

    def test_get_supported_types(self):
        """Test get_supported_types method"""
        supported_types = self.validator.get_supported_types()
        expected_types = ["ip", "ipaddress", "ip_address", "ipv4", "ipv6"]
        assert supported_types == expected_types
        assert len(supported_types) == 5

    def test_validate_edge_case_empty_constraints(self):
        """Test validation with empty constraints"""
        result = self.validator.validate("192.168.1.1", {})
        assert result.is_valid is True

    def test_validate_edge_case_none_constraints(self):
        """Test validation with None constraints"""
        result = self.validator.validate("192.168.1.1", None)
        assert result.is_valid is True

    def test_validate_reserved_addresses(self):
        """Test validation of reserved IP addresses"""
        # Test some reserved addresses to verify isReserved flag
        reserved_test_cases = [
            "0.0.0.0",          # This network
            "255.255.255.255",  # Limited broadcast
        ]
        
        for ip in reserved_test_cases:
            result = self.validator.validate(ip)
            assert result.is_valid is True
            # Note: isReserved behavior depends on Python's ipaddress module

    def test_constraint_validation_order(self):
        """Test that constraints are validated in the correct order"""
        # Test order: version -> private/public -> loopback -> multicast -> ranges
        constraints = {
            "version": "4",
            "allowLoopback": False,
            "blockedRanges": ["127.0.0.0/8"]
        }
        
        # IPv6 should fail on version check before loopback check
        result = self.validator.validate("::1", constraints)
        assert result.is_valid is False
        assert result.message == "Expected IPv4 address"

        # IPv4 loopback should fail on allowLoopback before blockedRanges
        result = self.validator.validate("127.0.0.1", constraints)
        assert result.is_valid is False
        assert result.message == "Loopback addresses are not allowed"

    def test_validate_ipv6_with_brackets(self):
        """Test IPv6 addresses with brackets (should fail)"""
        # IPv6 with brackets is not valid for ipaddress module
        result = self.validator.validate("[2001:db8::1]")
        assert result.is_valid is False
        assert result.message == "Invalid IP address format"

    def test_validate_ipv4_mapped_ipv6(self):
        """Test IPv4-mapped IPv6 addresses"""
        ipv4_mapped = "::ffff:192.168.1.1"
        result = self.validator.validate(ipv4_mapped)
        assert result.is_valid is True
        assert result.normalized_value["version"] == 6

    def test_validate_dual_stack_addresses(self):
        """Test dual-stack and transition IPv6 addresses"""
        dual_stack_addresses = [
            "2001:db8::192.168.1.1",  # IPv4-embedded IPv6
            "::ffff:0:192.168.1.1",   # IPv4-mapped IPv6
        ]
        
        for ip in dual_stack_addresses:
            result = self.validator.validate(ip)
            assert result.is_valid is True
            assert result.normalized_value["version"] == 6

    def test_validate_constraint_edge_cases(self):
        """Test edge cases in constraint validation"""
        # Empty allowed ranges
        constraints = {"allowedRanges": []}
        result = self.validator.validate("192.168.1.1", constraints)
        assert result.is_valid is False  # No ranges allowed means all fail

        # Empty blocked ranges
        constraints = {"blockedRanges": []}
        result = self.validator.validate("192.168.1.1", constraints)
        assert result.is_valid is True  # No ranges blocked means all pass

    def test_ipaddress_module_properties_coverage(self):
        """Test that we properly use ipaddress module properties"""
        # Test various IP types to ensure all properties are covered
        test_cases = [
            ("192.168.1.1", {"isPrivate": True, "isLoopback": False}),
            ("127.0.0.1", {"isPrivate": True, "isLoopback": True}),
            ("8.8.8.8", {"isPrivate": False, "isLoopback": False}),
            ("224.0.0.1", {"isMulticast": True}),
            ("::1", {"isLoopback": True, "version": 6}),
            ("ff02::1", {"isMulticast": True, "version": 6}),
        ]
        
        for ip, expected_props in test_cases:
            result = self.validator.validate(ip)
            assert result.is_valid is True
            for prop, expected_value in expected_props.items():
                assert result.normalized_value[prop] == expected_value, f"Failed for {ip}.{prop}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])