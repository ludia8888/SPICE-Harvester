"""
🔥 ULTRA: Adaptive Threshold Calculator
Intelligent threshold adjustment based on data quality metrics
"""

from typing import List, Dict
from dataclasses import dataclass


@dataclass
class DataQualityMetrics:
    """Data quality metrics for threshold calculation"""
    sample_size: int
    uniqueness_ratio: float
    null_ratio: float
    avg_length: float
    has_patterns: bool


class AdaptiveThresholdCalculator:
    """
    🔥 ULTRA: Calculate adaptive thresholds based on data characteristics
    
    Features:
    - Sample size-based adjustment
    - Data quality consideration
    - Type-specific threshold optimization
    - Pattern complexity analysis
    """
    
    # Base thresholds for each type
    BASE_THRESHOLDS = {
        'boolean': 0.9,
        'integer': 0.9,
        'decimal': 0.9,
        'date': 0.8,
        'datetime': 0.8,
        'phone': 0.7,
        'email': 0.8,
        'url': 0.8,
    }
    
    @classmethod
    def calculate_adaptive_thresholds(
        cls, 
        values: List[str], 
        sample_size: int
    ) -> Dict[str, float]:
        """
        Calculate adaptive thresholds based on data characteristics
        
        Args:
            values: Sample values for analysis
            sample_size: Total sample size
            
        Returns:
            Dictionary of adaptive thresholds by type
        """
        # Calculate data quality metrics
        metrics = cls._calculate_data_quality_metrics(values, sample_size)
        
        # Apply adjustments to base thresholds
        adaptive_thresholds = {}
        for type_name, base_threshold in cls.BASE_THRESHOLDS.items():
            adjusted = cls._adjust_threshold_for_type(
                base_threshold, metrics, type_name
            )
            adaptive_thresholds[type_name] = adjusted
            
        return adaptive_thresholds
    
    @classmethod
    def _calculate_data_quality_metrics(
        cls, 
        values: List[str], 
        sample_size: int
    ) -> DataQualityMetrics:
        """Calculate comprehensive data quality metrics"""
        
        if not values:
            return DataQualityMetrics(0, 0, 1, 0, False)
        
        # Basic metrics
        unique_count = len(set(values))
        total_count = len(values)
        uniqueness_ratio = unique_count / total_count
        
        # Null/empty ratio
        non_empty = [v for v in values if v and str(v).strip()]
        null_ratio = (total_count - len(non_empty)) / total_count
        
        # Average length
        avg_length = sum(len(str(v)) for v in non_empty) / len(non_empty) if non_empty else 0
        
        # Pattern detection (simplified)
        has_patterns = cls._detect_patterns(non_empty)
        
        return DataQualityMetrics(
            sample_size=sample_size,
            uniqueness_ratio=uniqueness_ratio,
            null_ratio=null_ratio,
            avg_length=avg_length,
            has_patterns=has_patterns
        )
    
    @classmethod
    def _detect_patterns(cls, values: List[str]) -> bool:
        """Detect if values have consistent patterns"""
        if len(values) < 3:
            return False
        
        # Check for consistent separators, lengths, formats
        separators = set()
        lengths = set()
        
        for value in values[:10]:  # Sample first 10 values
            # Check for separators
            for char in ['-', '.', '/', ' ', ':', '(', ')']:
                if char in value:
                    separators.add(char)
            
            # Check lengths
            lengths.add(len(value))
        
        # If most values have similar length or common separators, they have patterns
        return len(lengths) <= 3 or len(separators) > 0
    
    @classmethod
    def _adjust_threshold_for_type(
        cls, 
        base_threshold: float, 
        metrics: DataQualityMetrics, 
        type_name: str
    ) -> float:
        """Adjust threshold for specific type based on metrics"""
        
        adjustment = 0.0
        
        # Sample size adjustment
        if metrics.sample_size < 10:
            adjustment += 0.1  # Be more lenient with small samples
        elif metrics.sample_size > 1000:
            adjustment -= 0.05  # Be more strict with large samples
        
        # Uniqueness adjustment
        if metrics.uniqueness_ratio > 0.8:
            adjustment -= 0.05  # High uniqueness suggests complex data
        elif metrics.uniqueness_ratio < 0.3:
            adjustment += 0.05  # Low uniqueness suggests structured data
        
        # Null ratio adjustment
        if metrics.null_ratio > 0.3:
            adjustment += 0.05  # Many nulls suggest lower quality data
        
        # Pattern-based adjustment
        if metrics.has_patterns:
            if type_name in ['date', 'datetime', 'phone']:
                adjustment += 0.05  # Patterns help these types
            else:
                adjustment -= 0.02  # Might interfere with other types
        
        # Type-specific adjustments
        type_adjustments = {
            'boolean': 0.0,  # Boolean is usually clear-cut
            'phone': 0.1,   # Phone numbers vary greatly
            'email': 0.05,  # Email has some variation
        }
        
        adjustment += type_adjustments.get(type_name, 0.0)
        
        # Apply bounds
        final_threshold = max(0.5, min(0.98, base_threshold + adjustment))
        
        return final_threshold