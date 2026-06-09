import asyncio
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI

from ..config import settings
from ..core.state import app_state
from ..services.pubsub import run_pubsub_consumer
from ..services.websocket import ws_manager

logger = logging.getLogger(__name__)


async def _prune_stale_vehicles():
    while True:
        await asyncio.sleep(60)
        cutoff = time.time() - settings.vehicle_timeout_seconds
        stale = [vid for vid, ts in app_state.last_seen.items() if ts < cutoff]
        for vid in stale:
            app_state.vehicle_state.pop(vid, None)
            app_state.last_seen.pop(vid, None)
        if stale:
            logger.info("Pruned %d stale vehicles", len(stale))


async def _broadcast_stats():
    while True:
        await asyncio.sleep(settings.stats_broadcast_interval)
        now = time.time()
        active = app_state.active_vehicles(settings.vehicle_timeout_seconds)
        speeds = [v["Speed"] for v in active.values()]
        avg_speed = round(sum(speeds) / len(speeds), 1) if speeds else 0.0
        doors_open = sum(1 for v in active.values() if v["DoorStatus"] == 1)
        recent = [t for t in app_state.message_timestamps if t > now - 10]
        mps = round(len(recent) / 10)
        await ws_manager.broadcast({
            "type": "stats",
            "data": {
                "active_vehicles": len(active),
                "avg_speed": avg_speed,
                "messages_per_second": mps,
                "doors_open": doors_open,
            },
        })


async def _flush_vehicle_updates():
    while True:
        await asyncio.sleep(settings.vehicle_flush_interval)
        if not app_state.pending_updates:
            continue
        batch = dict(app_state.pending_updates)
        app_state.pending_updates.clear()
        for v in batch.values():
            await ws_manager.broadcast({"type": "vehicle_update", "data": v})


@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = [
        asyncio.create_task(run_pubsub_consumer(app_state, settings)),
        asyncio.create_task(_broadcast_stats()),
        asyncio.create_task(_flush_vehicle_updates()),
        asyncio.create_task(_prune_stale_vehicles()),
    ]
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
