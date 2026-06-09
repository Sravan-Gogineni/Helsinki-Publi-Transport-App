import random
import time

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from ...config import settings
from ...core.state import app_state
from ...models.vehicle import StatsResponse
from ...services.websocket import ws_manager

router = APIRouter()


def _compute_stats() -> dict:
    now = time.time()
    active = app_state.active_vehicles(settings.vehicle_timeout_seconds)
    speeds = [v["Speed"] for v in active.values()]
    avg_speed = round(sum(speeds) / len(speeds), 1) if speeds else 0.0
    doors_open = sum(1 for v in active.values() if v["DoorStatus"] == 1)
    recent = [t for t in app_state.message_timestamps if t > now - 10]
    mps = round(len(recent) / 10)
    return {
        "active_vehicles": len(active),
        "avg_speed": avg_speed,
        "messages_per_second": mps,
        "doors_open": doors_open,
    }


@router.get("/api/vehicles")
async def get_vehicles():
    active = list(app_state.active_vehicles(settings.vehicle_timeout_seconds).values())
    speeds = [v["Speed"] for v in active]
    avg_speed = round(sum(speeds) / len(speeds), 1) if speeds else 0.0
    return {"vehicles": active, "count": len(active), "avg_speed": avg_speed}


@router.get("/api/stats", response_model=StatsResponse)
async def get_stats():
    return _compute_stats()


@router.get("/api/simulate")
async def simulate():
    """Inject fake vehicle updates for local frontend testing."""
    helsinki_center = (60.1699, 24.9384)
    routes = ["1", "2", "4", "6", "M1", "506", "550", "200"]
    now = time.time()
    injected = []
    for i in range(5):
        vid = 9000 + i
        v = {
            "VehicleNumber": vid,
            "Latitude": helsinki_center[0] + random.uniform(-0.05, 0.05),
            "Longitude": helsinki_center[1] + random.uniform(-0.08, 0.08),
            "Speed": random.uniform(0, 80),
            "Heading": random.uniform(0, 359),
            "RouteNumber": random.choice(routes),
            "DoorStatus": random.choice([0, 0, 0, 1]),
            "OccupancyLevel": random.randint(0, 100),
            "Acceleration": random.uniform(-1, 1),
            "ScheduleOffset": random.randint(-120, 120),
            "Timestamp": "2026-06-07T11:04:14.251Z",
            "LocationSource": "GPS",
            "LineID": None,
            "OperatorID": 6,
        }
        app_state.vehicle_state[vid] = v
        app_state.last_seen[vid] = now
        app_state.message_timestamps.append(now)
        await ws_manager.broadcast({"type": "vehicle_update", "data": v})
        injected.append(vid)
    return {"injected": injected}
