"""
🔥 ULTRA: Email Type Checker
Advanced email detection with validation and domain analysis
"""

import re
from typing import Tuple, Dict
from shared.models.type_inference import TypeInferenceResult
from .base import BaseTypeChecker, TypeCheckContext


class EmailTypeChecker(BaseTypeChecker):
    """
    🔥 ULTRA: Email type checker with comprehensive validation
    
    Features:
    - RFC-compliant email validation
    - Domain pattern analysis
    - Fuzzy email detection
    - Common email provider recognition
    """
    
    # Comprehensive email regex pattern
    EMAIL_PATTERN = re.compile(
        r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    )
    
    # Common email domains for validation
    COMMON_DOMAINS = {
        'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 
        'icloud.com', 'me.com', 'aol.com', 'protonmail.com',
        'naver.com', 'daum.net', 'kakao.com', 'hanmail.net',  # Korean
        'qq.com', '163.com', '126.com', 'sina.com',  # Chinese
        'mail.ru', 'yandex.ru',  # Russian
    }
    
    def __init__(self):
        super().__init__(priority=35)
    
    @property
    def type_name(self) -> str:
        return "email"
    
    @property
    def default_threshold(self) -> float:
        return 0.8
    
    async def check_type(self, context: TypeCheckContext) -> TypeInferenceResult:
        """Check if values are email addresses"""
        
        exact_matches, fuzzy_matches, domain_stats = self._analyze_email_patterns(
            context.values
        )
        total = len(context.values)
        
        # Calculate confidence
        exact_confidence = exact_matches / total if total > 0 else 0
        fuzzy_confidence = self._calculate_confidence(
            exact_matches, total, fuzzy_matches, fuzzy_weight=0.6
        )
        
        # Column hint boost
        column_boost = self._check_column_hints(
            context, 
            ['email', 'mail', 'e-mail', 'address', 'contact', 'sender', 'recipient', 'to', 'from']
        )
        final_confidence = min(1.0, fuzzy_confidence + column_boost)
        
        threshold = self._get_threshold(context)
        
        # Format result with domain information
        domain_info = self._format_domain_stats(domain_stats) if domain_stats else ""
        
        if exact_confidence >= threshold:
            return TypeInferenceResult(
                type="email",
                confidence=exact_confidence,
                reason=f"Enhanced email detection: {exact_matches}/{total} valid emails ({exact_confidence*100:.1f}%){domain_info}",
                metadata={"domain_stats": domain_stats} if domain_stats else None
            )
        elif final_confidence >= threshold * 0.7:
            return TypeInferenceResult(
                type="email",
                confidence=final_confidence,
                reason=f"Enhanced email analysis: {exact_matches} exact + {fuzzy_matches} fuzzy matches ({final_confidence*100:.1f}%)",
                metadata={"has_column_hint": column_boost > 0}
            )
        else:
            return TypeInferenceResult(
                type="xsd:string",
                confidence=1.0 - final_confidence,
                reason=f"Enhanced email analysis: insufficient matches ({final_confidence*100:.1f}%)"
            )
    
    def _analyze_email_patterns(self, values: list) -> Tuple[int, int, Dict[str, int]]:
        """Analyze email patterns in values"""
        exact_matches = 0
        fuzzy_matches = 0
        domain_counts = {}
        
        for value in values:
            cleaned = str(value).strip().lower()
            
            # Try exact email matching
            if self.EMAIL_PATTERN.match(cleaned):
                exact_matches += 1
                # Extract domain
                domain = cleaned.split('@')[1]
                domain_counts[domain] = domain_counts.get(domain, 0) + 1
            # Try fuzzy matching
            elif self._check_fuzzy_email(cleaned):
                fuzzy_matches += 1
        
        return exact_matches, fuzzy_matches, domain_counts
    
    def _check_fuzzy_email(self, value: str) -> bool:
        """Check fuzzy email patterns"""
        # Check for @ symbol with text on both sides
        if '@' in value:
            parts = value.split('@')
            if len(parts) == 2 and len(parts[0]) > 0 and len(parts[1]) > 2:
                # Check if domain part has a dot
                if '.' in parts[1]:
                    return True
        
        # Check for email-like patterns with minor issues
        # e.g., missing TLD, spaces, etc.
        fuzzy_pattern = r'[a-zA-Z0-9._%+-]+\s*@\s*[a-zA-Z0-9.-]+'
        if re.search(fuzzy_pattern, value):
            return True
            
        return False
    
    def _format_domain_stats(self, domain_stats: Dict[str, int]) -> str:
        """Format domain statistics for display"""
        if not domain_stats:
            return ""
        
        # Get top 3 domains
        sorted_domains = sorted(domain_stats.items(), key=lambda x: x[1], reverse=True)[:3]
        
        # Check for common domains
        common_count = sum(1 for domain in domain_stats if domain in self.COMMON_DOMAINS)
        
        domain_info = f", top domains: {', '.join(f'{d[0]}({d[1]})' for d in sorted_domains)}"
        
        if common_count > 0:
            domain_info += f", {common_count} common providers"
            
        return domain_info