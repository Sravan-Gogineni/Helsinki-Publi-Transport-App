import time

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ...config import settings
from ...core.state import app_state
from ...services.websocket import ws_manager

router = APIRouter()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    now = time.time()
    cutoff = now - settings.vehicle_timeout_seconds
    state_copy = dict(app_state.vehicle_state)
    for vid, v in state_copy.items():
        if app_state.last_seen.get(vid, 0) > cutoff:
            try:
                await websocket.send_json({"type": "vehicle_update", "data": v})
            except Exception:
                break
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)
