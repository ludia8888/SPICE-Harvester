from shared.foundry.compute_routing import choose_search_around_backend, choose_writeback_backend


def test_choose_search_around_backend_prefers_index_when_under_threshold() -> None:
    decision = choose_search_around_backend(estimated_count=99_999, threshold=100_000)
    assert decision.route == "search_around"
    assert decision.selected_backend == "index_pruning"
    assert decision.spark_routed is False


def test_choose_search_around_backend_routes_spark_when_over_threshold() -> None:
    decision = choose_search_around_backend(estimated_count=100_001, threshold=100_000)
    assert decision.route == "search_around"
    assert decision.selected_backend == "spark_on_demand"
    assert decision.spark_routed is True


def test_choose_writeback_backend_routes_spark_when_over_threshold() -> None:
    decision = choose_writeback_backend(estimated_count=10_001, threshold=10_000)
    metadata = decision.as_metadata(
        execution_backend="index_pruning",
        fallback_reason="spark_runtime_not_integrated_using_index_pruning",
    )
    assert decision.route == "writeback"
    assert decision.selected_backend == "spark_on_demand"
    assert metadata["execution_backend"] == "index_pruning"
    assert metadata["fallback_reason"] == "spark_runtime_not_integrated_using_index_pruning"
