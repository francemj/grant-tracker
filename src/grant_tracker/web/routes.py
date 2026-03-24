from __future__ import annotations

import math

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from grant_tracker.db import GrantRepository
from grant_tracker.web.deps import get_db

router = APIRouter()

PER_PAGE = 24


def _templates(request: Request) -> Jinja2Templates:
    return request.app.state.templates


def _parse_int(value: str) -> int | None:
    """Parse a string to int, returning None for empty/invalid values."""
    if not value or not value.strip():
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


@router.get("/", response_class=HTMLResponse)
async def home(request: Request, db: GrantRepository = Depends(get_db)):
    stats = db.get_stats()
    featured = db.get_featured_grants(limit=6)
    categories = db.get_all_categories()
    return _templates(request).TemplateResponse(request, "home.html", {
        "stats": stats,
        "featured": featured,
        "categories": categories,
    })


@router.get("/grants", response_class=HTMLResponse)
async def grants_page(
    request: Request,
    q: str = "",
    categories: list[str] = Query(default=[]),
    provinces: list[str] = Query(default=[]),
    org_types: list[str] = Query(default=[]),
    accepting: bool = False,
    funding_min: str = "",
    funding_max: str = "",
    sort: str = "relevance",
    page: int = 1,
    db: GrantRepository = Depends(get_db),
):
    fmin = _parse_int(funding_min)
    fmax = _parse_int(funding_max)
    offset = (page - 1) * PER_PAGE
    results, total = db.search_grants_filtered(
        keyword=q,
        categories=categories or None,
        provinces=provinces or None,
        organization_types=org_types or None,
        accepting_only=accepting,
        funding_min=fmin,
        funding_max=fmax,
        sort=sort,
        limit=PER_PAGE,
        offset=offset,
    )
    total_pages = math.ceil(total / PER_PAGE) if total else 1
    all_categories = db.get_all_categories()

    ctx = {
        "grants": results,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "q": q,
        "selected_categories": categories,
        "selected_provinces": provinces,
        "selected_org_types": org_types,
        "accepting": accepting,
        "funding_min": fmin,
        "funding_max": fmax,
        "sort": sort,
        "all_categories": all_categories,
    }

    tmpl = _templates(request)
    is_htmx = request.headers.get("HX-Request") == "true"
    if is_htmx:
        return tmpl.TemplateResponse(request, "partials/grant_list.html", ctx)
    return tmpl.TemplateResponse(request, "grants.html", ctx)


@router.get("/grants/{grant_id}", response_class=HTMLResponse)
async def grant_detail(
    request: Request,
    grant_id: int,
    db: GrantRepository = Depends(get_db),
):
    grant = db.get_grant_by_id(grant_id)
    if not grant:
        return _templates(request).TemplateResponse(request, "404.html", status_code=404)
    similar = db.get_similar_grants(grant_id, limit=6)
    return _templates(request).TemplateResponse(request, "grant.html", {
        "grant": grant,
        "similar": similar,
    })


@router.get("/discover", response_class=HTMLResponse)
async def discover(request: Request):
    return _templates(request).TemplateResponse(request, "discover.html")


@router.get("/discover/results", response_class=HTMLResponse)
async def discover_results(
    request: Request,
    org_types: list[str] = Query(default=[]),
    provinces: list[str] = Query(default=[]),
    categories: list[str] = Query(default=[]),
    funding_min: str = "",
    funding_max: str = "",
    page: int = 1,
    db: GrantRepository = Depends(get_db),
):
    fmin = _parse_int(funding_min)
    fmax = _parse_int(funding_max)
    offset = (page - 1) * PER_PAGE
    results, total = db.search_grants_filtered(
        categories=categories or None,
        provinces=provinces or None,
        organization_types=org_types or None,
        accepting_only=True,
        funding_min=fmin,
        funding_max=fmax,
        sort="relevance",
        limit=PER_PAGE,
        offset=offset,
    )
    total_pages = math.ceil(total / PER_PAGE) if total else 1

    ctx = {
        "grants": results,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "q": "",
        "selected_categories": categories,
        "selected_provinces": provinces,
        "selected_org_types": org_types,
        "accepting": True,
        "funding_min": fmin,
        "funding_max": fmax,
        "sort": "relevance",
    }

    tmpl = _templates(request)
    is_htmx = request.headers.get("HX-Request") == "true"
    if is_htmx:
        return tmpl.TemplateResponse(request, "partials/grant_list.html", ctx)
    return tmpl.TemplateResponse(request, "discover_results.html", ctx)
