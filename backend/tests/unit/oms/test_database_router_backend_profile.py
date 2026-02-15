from __future__ import annotations

from oms.routers import database as database_router


def test_oms_database_routes_are_foundry_style_without_profile_gates() -> None:
    route_paths = {route.path for route in database_router.router.routes}
    assert "/database/list" in route_paths
    assert "/database/create" in route_paths
    assert "/database/exists/{db_name}" in route_paths
