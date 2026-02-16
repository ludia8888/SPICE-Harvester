"""
🔥 THINK ULTRA! Type Inference Service Dependencies
Provides dependency injection configuration for type inference services
"""

from typing import Optional

from ..interfaces.type_inference import TypeInferenceInterface

# Global singleton for type inference service
_type_inference_service: Optional[TypeInferenceInterface] = None


def configure_type_inference_service(service: TypeInferenceInterface) -> None:
    """
    Configure the type inference service implementation.

    This should be called during application startup to set the
    concrete implementation of the type inference service.

    Args:
        service: An instance implementing TypeInferenceInterface
    """
    global _type_inference_service
    _type_inference_service = service


def get_type_inference_service() -> TypeInferenceInterface:
    """
    Get the configured type inference service.

    This function is used with FastAPI's Depends() for dependency injection.

    Returns:
        The configured type inference service

    Raises:
        RuntimeError: If the service has not been configured
    """
    if _type_inference_service is None:
        raise RuntimeError(
            "Type inference service not configured. "
            "Call configure_type_inference_service() during application startup."
        )
    return _type_inference_service


# For FastAPI dependency injection
def type_inference_dependency() -> TypeInferenceInterface:
    """
    FastAPI dependency function for type inference service.

    Usage:
        @router.post("/analyze")
        async def analyze_data(
            service: TypeInferenceInterface = Depends(type_inference_dependency)
        ):
            ...
    """
    return get_type_inference_service()


def reset_type_inference_service() -> None:
    """
    Reset the type inference service (mainly for testing).
    """
    global _type_inference_service
    _type_inference_service = None


if __name__ == "__main__":
    class _DummyTypeInferenceService(TypeInferenceInterface):
        async def infer_column_type(self, column_data, column_name=None, include_complex_types=False, context_columns=None, metadata=None):
            from shared.models.type_inference import ColumnAnalysisResult, TypeInferenceResult

            return ColumnAnalysisResult(
                column_name=column_name or "col",
                inferred_type=TypeInferenceResult(type="xsd:string", confidence=1.0, reason="dummy"),
                total_count=len(column_data),
                non_empty_count=len(column_data),
                sample_values=list(column_data)[:10],
                null_count=0,
                unique_count=len(set(str(v) for v in column_data)),
                null_ratio=0.0,
                unique_ratio=1.0,
            )

        async def analyze_dataset(self, data, columns, sample_size=1000, include_complex_types=False, metadata=None):
            return [await self.infer_column_type([row[idx] if idx < len(row) else None for row in data], column_name=col) for idx, col in enumerate(columns)]

    # Test configuration
    configure_type_inference_service(_DummyTypeInferenceService())

    # Test retrieval
    retrieved_service = get_type_inference_service()
    print(f"Service configured: {retrieved_service is not None}")
    print(f"Service type: {type(retrieved_service)}")

    # Test dependency function
    dependency_service = type_inference_dependency()
    print(f"Dependency service: {dependency_service is not None}")
    print(f"Same instance: {dependency_service is retrieved_service}")

    # Test reset
    reset_type_inference_service()
    print("Service reset")

    try:
        get_type_inference_service()
        print("ERROR: Should have raised RuntimeError")
    except RuntimeError as e:
        print(f"Correctly raised error: {e}")
