"""
🔥 ULTRA: Base Type Checker Interface
Enterprise-grade type inference with SOLID principles
"""

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict
from dataclasses import dataclass

from shared.models.type_inference import TypeInferenceResult


@dataclass
class TypeCheckContext:
    """Context for type checking with all necessary information"""
    values: List[str]
    column_name: Optional[str] = None
    sample_size: int = 0
    context_columns: Optional[Dict[str, List[Any]]] = None
    adaptive_thresholds: Optional[Dict[str, float]] = None


class BaseTypeChecker(ABC):
    """
    🔥 ULTRA: Abstract base class for all type checkers
    
    Implements SOLID principles:
    - SRP: Each checker handles only one type
    - OCP: Easy to extend with new type checkers
    - LSP: All checkers are interchangeable
    - ISP: Minimal interface
    - DIP: Depend on abstraction, not concretion
    """
    
    def __init__(self, priority: int = 50):
        """
        Initialize type checker with priority
        
        Args:
            priority: Lower numbers = higher priority (0-100)
        """
        self.priority = priority
    
    @property
    @abstractmethod
    def type_name(self) -> str:
        """Return the type this checker identifies"""
        pass
    
    @property
    @abstractmethod 
    def default_threshold(self) -> float:
        """Return default confidence threshold for this type"""
        pass
    
    @abstractmethod
    async def check_type(self, context: TypeCheckContext) -> TypeInferenceResult:
        """
        Check if values match this type
        
        Args:
            context: All information needed for type checking
            
        Returns:
            TypeInferenceResult with confidence and reasoning
        """
        pass
    
    def _calculate_confidence(self, matched: int, total: int, 
                            fuzzy_matched: int = 0, fuzzy_weight: float = 0.8) -> float:
        """Helper method to calculate confidence scores"""
        if total == 0:
            return 0.0
        return (matched + fuzzy_matched * fuzzy_weight) / total
    
    def _get_threshold(self, context: TypeCheckContext) -> float:
        """Get adaptive threshold for this type"""
        if context.adaptive_thresholds and self.type_name.lower() in context.adaptive_thresholds:
            return context.adaptive_thresholds[self.type_name.lower()]
        return self.default_threshold
    
    def _check_column_hints(self, context: TypeCheckContext, keywords: List[str]) -> float:
        """Check if column name suggests this type"""
        if not context.column_name:
            return 0.0
            
        column_lower = context.column_name.lower()
        for keyword in keywords:
            if keyword in column_lower:
                return 0.1  # Boost confidence by 10%
        return 0.0


@dataclass
class TypeCheckResult:
    """Result container for parallel type checking"""
    checker_name: str
    result: TypeInferenceResult
    priority: int = 0  # Will be set by the manager
    
    def __lt__(self, other):
        """For sorting by confidence (descending) then priority (ascending)"""
        if self.result.confidence != other.result.confidence:
            return self.result.confidence > other.result.confidence
        return self.priority < other.priority