from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from grant_tracker.db import DEFAULT_DB_PATH, GrantRepository
from grant_tracker.models import (
    CATEGORY_TAXONOMY,
    ORGANIZATION_TYPE_TAXONOMY,
    PROVINCE_CODES,
)
from grant_tracker.web.routes import router

TEMPLATES_DIR = Path(__file__).parent / "templates"

PROVINCE_NAMES = {
    "AB": "Alberta", "BC": "British Columbia", "MB": "Manitoba",
    "NB": "New Brunswick", "NL": "Newfoundland and Labrador",
    "NS": "Nova Scotia", "NT": "Northwest Territories", "NU": "Nunavut",
    "ON": "Ontario", "PE": "Prince Edward Island", "QC": "Quebec",
    "SK": "Saskatchewan", "YT": "Yukon", "ALL": "All of Canada",
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = GrantRepository(DEFAULT_DB_PATH)
    yield
    app.state.db.close()


def create_app() -> FastAPI:
    app = FastAPI(title="Canadian Grant Finder", lifespan=lifespan)
    app.include_router(router)
    static_dir = Path(__file__).parent / "static"
    if static_dir.is_dir():
        app.mount("/static", StaticFiles(directory=static_dir), name="static")

    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    templates.env.globals.update(
        province_names=PROVINCE_NAMES,
        category_labels={c: c.replace("-", " ").title() for c in CATEGORY_TAXONOMY},
        org_type_labels={o: o.replace("-", " ").title() for o in ORGANIZATION_TYPE_TAXONOMY},
        category_taxonomy=CATEGORY_TAXONOMY,
        org_type_taxonomy=ORGANIZATION_TYPE_TAXONOMY,
        province_codes=[c for c in PROVINCE_CODES if c != "ALL"],
    )
    app.state.templates = templates

    @app.exception_handler(404)
    async def not_found(request, exc):
        return templates.TemplateResponse(request, "404.html", status_code=404)

    return app
