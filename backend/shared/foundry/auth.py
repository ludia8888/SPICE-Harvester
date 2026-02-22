from __future__ import annotations

from typing import Iterable, Sequence

from fastapi import Depends, Request

from shared.foundry.errors import FoundryAPIError
from shared.security.user_context import UserPrincipal


_UNAUTHORIZED_ERROR_CODE = "UNAUTHORIZED"
_MISSING_CREDENTIALS_ERROR_NAME = "MissingCredentials"
_PERMISSION_DENIED_ERROR_CODE = "PERMISSION_DENIED"
_API_USAGE_DENIED_ERROR_NAME = "ApiUsageDenied"


def _principal_scopes(principal: object) -> set[str]:
    if not isinstance(principal, UserPrincipal):
        return set()
    scopes = principal.scopes()
    return {scope for scope in scopes if scope}


def require_scopes(
    required: Sequence[str],
) -> Depends:
    required_set = {str(scope).strip() for scope in required if str(scope).strip()}
    required_sorted = tuple(sorted(required_set))

    async def _dependency(request: Request) -> None:
        principal = getattr(request.state, "user", None)
        if principal is None:
            raise FoundryAPIError(
                status=401,
                error_code=_UNAUTHORIZED_ERROR_CODE,
                error_name=_MISSING_CREDENTIALS_ERROR_NAME,
                parameters={},
            )

        principal_scopes = _principal_scopes(principal)
        missing = sorted(scope for scope in required_set if scope not in principal_scopes)
        if missing:
            parameters: dict[str, object] = {}
            if len(missing) == 1:
                parameters["missingScope"] = missing[0]
            else:
                parameters["missingScopes"] = missing
            raise FoundryAPIError(
                status=403,
                error_code=_PERMISSION_DENIED_ERROR_CODE,
                error_name=_API_USAGE_DENIED_ERROR_NAME,
                parameters=parameters,
            )

    setattr(_dependency, "__foundry_required_scopes__", required_sorted)
    return Depends(_dependency)
