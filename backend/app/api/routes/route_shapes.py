import asyncio
import logging

import httpx
from fastapi import APIRouter, HTTPException

from ...config import settings

router = APIRouter()
logger = logging.getLogger(__name__)

_ENDPOINT = "https://api.digitransit.fi/routing/v2/hsl/gtfs/v1"
_SEMAPHORE = asyncio.Semaphore(3)   # max 3 concurrent Digitransit requests

# Positive cache: route_number -> [[lat, lng], ...]
_shape_cache: dict[str, list] = {}
# Negative cache: routes with no shape in Digitransit (don't retry)
_no_shape: set[str] = set()

_QUERY = '{routes(ids:["%s"]){patterns{geometry{lat lon}}}}'


def _candidate_ids(route_number: str) -> list[str]:
    """Build candidate HSL GTFS IDs from a short route name."""
    candidates = []
    try:
        n = int(route_number)
        if 1 <= n <= 99:
            candidates.append(f"HSL:10{n:02d}")       # 42  → HSL:1042
        elif 100 <= n <= 999:
            candidates.append(f"HSL:1{n}")             # 550 → HSL:1550
            candidates.append(f"HSL:2{n}")             # 550 → HSL:2550
        elif 1000 <= n <= 9999:
            candidates.append(f"HSL:{n}")              # already JORE
    except ValueError:
        upper = route_number.upper()
        if upper.startswith("M"):
            candidates.append(f"HSL:31{upper}")        # M1 → HSL:31M1
        else:
            candidates.append(f"HSL:{route_number}")   # train letters, etc.
    return candidates


async def _fetch_one(client: httpx.AsyncClient, hsl_id: str, headers: dict) -> list | None:
    """Fetch geometry for a single HSL route ID. Returns latlngs or None."""
    async with _SEMAPHORE:
        try:
            resp = await client.post(
                _ENDPOINT,
                json={"query": _QUERY % hsl_id},
                headers=headers,
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.debug("Digitransit HTTP %s for %s", e.response.status_code, hsl_id)
            return None
        except Exception as e:
            logger.warning("Digitransit error for %s: %s", hsl_id, e)
            return None

    routes = (resp.json().get("data") or {}).get("routes") or []
    route = next((r for r in routes if r), None)
    if not route:
        return None

    patterns = route.get("patterns") or []
    if not patterns:
        return None

    best = max(patterns, key=lambda p: len(p.get("geometry") or []))
    geometry = best.get("geometry") or []
    if not geometry:
        return None

    return [[pt["lat"], pt["lon"]] for pt in geometry]


@router.get("/api/route-shape/{route_number}")
async def get_route_shape(route_number: str):
    if route_number in _shape_cache:
        return {"coordinates": _shape_cache[route_number]}

    if route_number in _no_shape:
        raise HTTPException(status_code=404, detail=f"No shape for route {route_number}")

    headers = {
        "Content-Type": "application/json",
        "digitransit-subscription-key": settings.digitransit_api_key,
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        for hsl_id in _candidate_ids(route_number):
            latlngs = await _fetch_one(client, hsl_id, headers)
            if latlngs:
                _shape_cache[route_number] = latlngs
                return {"coordinates": latlngs}

    _no_shape.add(route_number)
    raise HTTPException(status_code=404, detail=f"No shape found for route {route_number}")
