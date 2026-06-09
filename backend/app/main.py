import logging
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from .api.routes import vehicles, ws
from .config import settings
from .core.lifespan import lifespan

logging.basicConfig(level=logging.INFO)

_FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "frontend")
_FRONTEND_INDEX = os.path.join(_FRONTEND_DIR, "index.html")


def create_app() -> FastAPI:
    app = FastAPI(title="HFP Live Dashboard", lifespan=lifespan)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.mount("/static", StaticFiles(directory=_FRONTEND_DIR), name="static")

    app.include_router(vehicles.router)
    app.include_router(ws.router)

    @app.get("/")
    async def serve_dashboard():
        try:
            with open(_FRONTEND_INDEX) as f:
                html = f.read()
        except FileNotFoundError:
            return HTMLResponse("<h1>Frontend not found</h1>", status_code=404)
        html = html.replace("{{MAPS_KEY}}", settings.google_maps_api_key)
        return HTMLResponse(html)

    return app


app = create_app()
