from __future__ import annotations

from fastapi import Request

from grant_tracker.db import GrantRepository


def get_db(request: Request) -> GrantRepository:
    return request.app.state.db
