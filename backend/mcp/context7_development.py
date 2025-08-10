"""
Context7 Development Helper
Utilities for using Context7 during development
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from backend.mcp.mcp_client import get_context7_client, Context7Client

logger = logging.getLogger(__name__)


class Context7Developer:
    """
    Development helper that integrates Context7 for code analysis and suggestions
    """
    
    def __init__(self):
        self.client = get_context7_client()
        self.cache = {}
        
    async def analyze_before_implementation(
        self,
        feature_name: str,
        description: str,
        related_files: List[str] = None
    ) -> Dict[str, Any]:
        """
        Analyze codebase before implementing a new feature
        
        Args:
            feature_name: Name of the feature to implement
            description: Description of what needs to be done
            related_files: List of files that might be related
            
        Returns:
            Analysis results with patterns, suggestions, and warnings
        """
        try:
            # Search for similar implementations
            similar_patterns = await self.client.search(
                f"similar implementation: {description}",
                limit=5
            )
            
            # Get architectural insights
            architecture_query = f"architecture pattern for: {feature_name}"
            architecture_insights = await self.client.search(
                architecture_query,
                limit=3
            )
            
            # Check for potential issues
            issues_query = f"potential issues when implementing: {description}"
            potential_issues = await self.client.search(
                issues_query,
                limit=3
            )
            
            # Compile analysis
            analysis = {
                "feature": feature_name,
                "timestamp": datetime.utcnow().isoformat(),
                "similar_patterns": similar_patterns,
                "architecture_insights": architecture_insights,
                "potential_issues": potential_issues,
                "recommendations": self._generate_recommendations(
                    similar_patterns,
                    architecture_insights
                )
            }
            
            # Store in Context7 for future reference
            await self.client.add_knowledge(
                title=f"Pre-implementation Analysis: {feature_name}",
                content=str(analysis),
                metadata={
                    "type": "analysis",
                    "feature": feature_name,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze before implementation: {e}")
            return {
                "error": str(e),
                "feature": feature_name,
                "fallback": True
            }
    
    async def validate_implementation(
        self,
        feature_name: str,
        implementation_details: Dict[str, Any],
        files_modified: List[str]
    ) -> Dict[str, Any]:
        """
        Validate implementation against best practices
        
        Args:
            feature_name: Name of the implemented feature
            implementation_details: Details about the implementation
            files_modified: List of modified files
            
        Returns:
            Validation results with suggestions
        """
        try:
            # Check implementation against patterns
            validation_query = f"validate implementation: {feature_name} with details: {implementation_details}"
            validation_results = await self.client.search(
                validation_query,
                limit=5
            )
            
            # Get improvement suggestions
            suggestions = await self._get_improvement_suggestions(
                feature_name,
                files_modified
            )
            
            # Check for code smells
            code_smells = await self._check_code_smells(
                implementation_details
            )
            
            validation = {
                "feature": feature_name,
                "timestamp": datetime.utcnow().isoformat(),
                "validation_results": validation_results,
                "suggestions": suggestions,
                "code_smells": code_smells,
                "overall_score": self._calculate_quality_score(
                    validation_results,
                    code_smells
                )
            }
            
            return validation
            
        except Exception as e:
            logger.error(f"Failed to validate implementation: {e}")
            return {
                "error": str(e),
                "feature": feature_name,
                "fallback": True
            }
    
    async def document_implementation(
        self,
        feature_name: str,
        description: str,
        technical_details: Dict[str, Any],
        lessons_learned: List[str] = None
    ) -> Dict[str, Any]:
        """
        Document implementation in Context7 knowledge base
        
        Args:
            feature_name: Name of the feature
            description: Description of the implementation
            technical_details: Technical implementation details
            lessons_learned: Lessons learned during implementation
            
        Returns:
            Documentation result
        """
        try:
            # Prepare documentation content
            content = f"""
# Feature: {feature_name}

## Description
{description}

## Technical Details
{self._format_technical_details(technical_details)}

## Implementation Date
{datetime.utcnow().isoformat()}

## Lessons Learned
{self._format_lessons(lessons_learned)}
"""
            
            # Add to Context7 knowledge base
            result = await self.client.add_knowledge(
                title=f"Implementation: {feature_name}",
                content=content,
                metadata={
                    "type": "implementation",
                    "feature": feature_name,
                    "timestamp": datetime.utcnow().isoformat(),
                    "technical_details": technical_details
                }
            )
            
            # Link to related components
            if "related_components" in technical_details:
                for component in technical_details["related_components"]:
                    await self.client.link_entities(
                        source_id=result.get("id"),
                        target_id=component,
                        relationship="implements"
                    )
            
            return {
                "documented": True,
                "knowledge_id": result.get("id"),
                "feature": feature_name
            }
            
        except Exception as e:
            logger.error(f"Failed to document implementation: {e}")
            return {
                "error": str(e),
                "feature": feature_name,
                "documented": False
            }
    
    async def get_coding_suggestions(
        self,
        code_snippet: str,
        language: str = "python",
        context: str = ""
    ) -> List[str]:
        """
        Get coding suggestions from Context7
        
        Args:
            code_snippet: Code to get suggestions for
            language: Programming language
            context: Additional context
            
        Returns:
            List of suggestions
        """
        try:
            query = f"improve {language} code: {code_snippet[:200]} context: {context}"
            results = await self.client.search(query, limit=5)
            
            suggestions = []
            for result in results:
                if "suggestion" in result:
                    suggestions.append(result["suggestion"])
                    
            return suggestions
            
        except Exception as e:
            logger.error(f"Failed to get coding suggestions: {e}")
            return []
    
    def _generate_recommendations(
        self,
        patterns: List[Dict],
        insights: List[Dict]
    ) -> List[str]:
        """Generate recommendations based on patterns and insights"""
        recommendations = []
        
        # Based on similar patterns
        if patterns:
            recommendations.append(
                f"Consider following the pattern used in: {patterns[0].get('title', 'similar implementation')}"
            )
        
        # Based on architecture insights
        if insights:
            recommendations.append(
                f"Architecture suggestion: {insights[0].get('content', 'Follow SOLID principles')}"
            )
        
        # Default recommendations
        recommendations.extend([
            "Ensure proper error handling",
            "Add comprehensive logging",
            "Write unit tests for new functionality",
            "Update documentation"
        ])
        
        return recommendations
    
    async def _get_improvement_suggestions(
        self,
        feature_name: str,
        files_modified: List[str]
    ) -> List[str]:
        """Get improvement suggestions for the implementation"""
        suggestions = []
        
        for file in files_modified[:3]:  # Limit to avoid too many queries
            query = f"improve {file} for feature {feature_name}"
            results = await self.client.search(query, limit=2)
            
            for result in results:
                if "suggestion" in result:
                    suggestions.append(f"{file}: {result['suggestion']}")
                    
        return suggestions
    
    async def _check_code_smells(
        self,
        implementation_details: Dict[str, Any]
    ) -> List[str]:
        """Check for potential code smells"""
        code_smells = []
        
        # Check for common issues
        if "functions" in implementation_details:
            for func in implementation_details["functions"]:
                if len(func.get("parameters", [])) > 5:
                    code_smells.append(f"Function {func['name']} has too many parameters")
                    
        if "classes" in implementation_details:
            for cls in implementation_details["classes"]:
                if len(cls.get("methods", [])) > 20:
                    code_smells.append(f"Class {cls['name']} might be too complex")
                    
        return code_smells
    
    def _calculate_quality_score(
        self,
        validation_results: List[Dict],
        code_smells: List[str]
    ) -> float:
        """Calculate overall quality score"""
        score = 100.0
        
        # Deduct for code smells
        score -= len(code_smells) * 5
        
        # Add for positive validation
        score += len(validation_results) * 2
        
        # Ensure score is between 0 and 100
        return max(0, min(100, score))
    
    def _format_technical_details(self, details: Dict[str, Any]) -> str:
        """Format technical details for documentation"""
        formatted = []
        for key, value in details.items():
            formatted.append(f"- **{key}**: {value}")
        return "\n".join(formatted)
    
    def _format_lessons(self, lessons: Optional[List[str]]) -> str:
        """Format lessons learned"""
        if not lessons:
            return "- No specific lessons documented"
        return "\n".join(f"- {lesson}" for lesson in lessons)


# Singleton instance
_developer = None


def get_context7_developer() -> Context7Developer:
    """Get or create Context7 developer instance"""
    global _developer
    if _developer is None:
        _developer = Context7Developer()
    return _developer


# Convenience functions for development
async def analyze_feature(name: str, description: str) -> Dict[str, Any]:
    """Quick analysis before implementing a feature"""
    developer = get_context7_developer()
    return await developer.analyze_before_implementation(name, description)


async def validate_code(feature: str, details: Dict, files: List[str]) -> Dict[str, Any]:
    """Quick validation of implementation"""
    developer = get_context7_developer()
    return await developer.validate_implementation(feature, details, files)


async def document_feature(
    name: str,
    description: str,
    details: Dict,
    lessons: List[str] = None
) -> Dict[str, Any]:
    """Quick documentation of implemented feature"""
    developer = get_context7_developer()
    return await developer.document_implementation(name, description, details, lessons)