"""Cloudflare Access JWT verification as a FastAPI dependency."""

from __future__ import annotations

import json
import logging
import time
from typing import Annotated, Any

import aiohttp
import jwt
from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyCookie, APIKeyHeader

from app.config import settings

logger = logging.getLogger(__name__)

_JWKS_CACHE: dict[str, Any] = {"keys": [], "fetched_at": 0.0}
_JWKS_TTL_SEC = 3600.0

# OpenAPI security schemes → Swagger "Authorize" inputs
cf_jwt_header = APIKeyHeader(
    name="Cf-Access-Jwt-Assertion",
    auto_error=False,
    description="Cloudflare Access JWT (preferred for Swagger testing)",
)
cf_jwt_cookie = APIKeyCookie(
    name="CF_Authorization",
    auto_error=False,
    description="Cloudflare Access JWT cookie set by Access login",
)


async def _fetch_public_keys() -> list[Any]:
    """Fetch and cache Cloudflare Access JWKS public keys."""
    now = time.monotonic()
    if _JWKS_CACHE["keys"] and (now - _JWKS_CACHE["fetched_at"]) < _JWKS_TTL_SEC:
        return _JWKS_CACHE["keys"]

    certs_url = settings.cf_certs_url
    if not certs_url:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Cloudflare Access team domain is not configured",
        )

    timeout = aiohttp.ClientTimeout(total=10.0)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(certs_url) as response:
            response.raise_for_status()
            jwk_set = await response.json()

    public_keys: list[Any] = []
    for key_dict in jwk_set.get("keys", []):
        public_keys.append(jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(key_dict)))

    _JWKS_CACHE["keys"] = public_keys
    _JWKS_CACHE["fetched_at"] = now
    return public_keys


def _extract_token(
    cf_authorization: str | None,
    cf_access_jwt_assertion: str | None,
) -> str:
    token = cf_authorization or cf_access_jwt_assertion
    if not token:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="missing required cf authorization token",
        )
    return token


async def require_cf_access(
    cf_authorization: Annotated[str | None, Security(cf_jwt_cookie)] = None,
    cf_access_jwt_assertion: Annotated[str | None, Security(cf_jwt_header)] = None,
) -> dict[str, Any] | None:
    """
    Verify Cloudflare Access JWT and return claims.

    When CF Access is not configured (local/dev), this is a no-op and returns None.
    """
    if not settings.cf_access_enabled:
        logger.info("CF Access is not configured, skipping JWT verification")
        return None

    if not settings.policy_aud:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="missing required audience",
        )

    token = _extract_token(cf_authorization, cf_access_jwt_assertion)

    try:
        keys = await _fetch_public_keys()
    except (aiohttp.ClientError, TimeoutError) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"failed to fetch Cloudflare Access certs: {exc}",
        ) from exc

    for key in keys:
        try:
            return jwt.decode(
                token,
                key=key,
                audience=settings.policy_aud,
                algorithms=["RS256"],
            )
        except jwt.PyJWTError:
            continue

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="invalid token",
    )


CFAccessClaims = Annotated[dict[str, Any] | None, Depends(require_cf_access)]
