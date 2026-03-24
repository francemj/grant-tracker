from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path

from grant_tracker.models import FundingLevel, Grant

DEFAULT_DB_PATH = Path(os.environ.get("GRANT_DB_PATH", "grants.db"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS grants (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    title           TEXT NOT NULL,
    organization    TEXT NOT NULL,
    url             TEXT NOT NULL DEFAULT '',
    description     TEXT NOT NULL DEFAULT '',
    deadline        TEXT,
    funding_min     INTEGER,
    funding_max     INTEGER,
    eligibility     TEXT,
    funding_level   TEXT NOT NULL DEFAULT 'federal',
    contact_info    TEXT,
    source          TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    status          TEXT,
    last_crawled    TEXT NOT NULL,
    raw_text        TEXT NOT NULL DEFAULT '',
    raw_text_hash   TEXT NOT NULL DEFAULT '',
    enriched        INTEGER NOT NULL DEFAULT 0,
    relevance_score REAL NOT NULL DEFAULT 0.0,
    accepting_applications INTEGER NOT NULL DEFAULT 0,
    is_applyable_grant INTEGER NOT NULL DEFAULT 1,
    categories      TEXT NOT NULL DEFAULT '[]',
    provinces       TEXT NOT NULL DEFAULT '[]',
    organization_types TEXT NOT NULL DEFAULT '[]',
    UNIQUE(source, source_id)
);
"""

MIGRATIONS = [
    "ALTER TABLE grants ADD COLUMN raw_text TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE grants ADD COLUMN raw_text_hash TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE grants ADD COLUMN enriched INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE grants ADD COLUMN relevance_score REAL NOT NULL DEFAULT 0.0",
    "ALTER TABLE grants ADD COLUMN accepting_applications INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE grants ADD COLUMN is_applyable_grant INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE grants ADD COLUMN categories TEXT NOT NULL DEFAULT '[]'",
    "ALTER TABLE grants ADD COLUMN provinces TEXT NOT NULL DEFAULT '[]'",
    "ALTER TABLE grants ADD COLUMN organization_types TEXT NOT NULL DEFAULT '[]'",
]

_UPSERT_SQL = """
INSERT INTO grants (
    title, organization, url, description, deadline,
    funding_min, funding_max, eligibility, funding_level,
    contact_info, source, source_id, status, last_crawled,
    raw_text, raw_text_hash, enriched, relevance_score,
    accepting_applications, is_applyable_grant,
    categories, provinces, organization_types
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(source, source_id) DO UPDATE SET
    title        = excluded.title,
    organization = excluded.organization,
    url          = excluded.url,
    description  = excluded.description,
    deadline     = excluded.deadline,
    funding_min  = excluded.funding_min,
    funding_max  = excluded.funding_max,
    eligibility  = excluded.eligibility,
    funding_level = excluded.funding_level,
    contact_info = excluded.contact_info,
    status       = excluded.status,
    last_crawled = excluded.last_crawled,
    raw_text     = excluded.raw_text,
    raw_text_hash = excluded.raw_text_hash,
    enriched     = excluded.enriched,
    relevance_score = excluded.relevance_score,
    accepting_applications = excluded.accepting_applications,
    is_applyable_grant = excluded.is_applyable_grant,
    categories   = excluded.categories,
    provinces    = excluded.provinces,
    organization_types = excluded.organization_types
"""


class GrantRepository:
    def __init__(self, db_path: Path = DEFAULT_DB_PATH) -> None:
        self.db_path = db_path
        self._conn = sqlite3.connect(str(db_path))
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(SCHEMA)
        self._run_migrations()

    def _run_migrations(self) -> None:
        for sql in MIGRATIONS:
            try:
                self._conn.execute(sql)
            except sqlite3.OperationalError:
                pass  # column already exists

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def upsert_grant(self, grant: Grant) -> None:
        self._conn.execute(
            _UPSERT_SQL,
            (
                grant.title,
                grant.organization,
                grant.url,
                grant.description,
                grant.deadline,
                grant.funding_min,
                grant.funding_max,
                grant.eligibility,
                grant.funding_level.value,
                grant.contact_info,
                grant.source,
                grant.source_id,
                grant.status,
                grant.last_crawled.isoformat(),
                grant.raw_text,
                grant.raw_text_hash,
                1 if grant.enriched else 0,
                grant.relevance_score,
                1 if grant.accepting_applications else 0,
                1 if grant.is_applyable_grant else 0,
                json.dumps(grant.categories),
                json.dumps(grant.provinces),
                json.dumps(grant.organization_types),
            ),
        )
        self._conn.commit()

    def upsert_many(self, grants: list[Grant]) -> int:
        count = 0
        for grant in grants:
            self.upsert_grant(grant)
            count += 1
        return count

    def delete_by_source_keys(self, keys: list[tuple[str, str]]) -> int:
        """Delete grants by (source, source_id). Returns number of deleted rows."""
        if not keys:
            return 0
        cur = self._conn.cursor()
        cur.executemany(
            "DELETE FROM grants WHERE source = ? AND source_id = ?",
            keys,
        )
        self._conn.commit()
        return cur.rowcount

    # ------------------------------------------------------------------
    # Read operations (existing)
    # ------------------------------------------------------------------

    def get_grants(
        self,
        *,
        source: str | None = None,
        status: str | None = None,
        funding_level: FundingLevel | None = None,
    ) -> list[Grant]:
        query = "SELECT * FROM grants WHERE 1=1"
        params: list[str] = []

        if source:
            query += " AND source = ?"
            params.append(source)
        if status:
            query += " AND LOWER(status) LIKE ?"
            params.append(f"%{status.lower()}%")
        if funding_level:
            query += " AND funding_level = ?"
            params.append(funding_level.value)

        query += " ORDER BY last_crawled DESC"
        rows = self._conn.execute(query, params).fetchall()
        return [_row_to_grant(row) for row in rows]

    def search_grants(self, keyword: str) -> list[Grant]:
        query = """
            SELECT * FROM grants
            WHERE LOWER(title) LIKE ?
               OR LOWER(description) LIKE ?
               OR LOWER(organization) LIKE ?
               OR LOWER(eligibility) LIKE ?
            ORDER BY last_crawled DESC
        """
        term = f"%{keyword.lower()}%"
        rows = self._conn.execute(query, (term, term, term, term)).fetchall()
        return [_row_to_grant(row) for row in rows]

    def count(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) FROM grants").fetchone()
        return row[0]

    def get_unenriched_grants(self) -> list[Grant]:
        rows = self._conn.execute(
            "SELECT * FROM grants WHERE enriched = 0 AND raw_text != '' ORDER BY source"
        ).fetchall()
        return [_row_to_grant(row) for row in rows]

    def get_unenriched_grants_for_sources(self, sources: tuple[str, ...]) -> list[Grant]:
        """Return unenriched grants for specific sources (raw_text required)."""
        if not sources:
            return []
        placeholders = ",".join("?" * len(sources))
        query = (
            f"SELECT * FROM grants WHERE enriched = 0 AND raw_text != '' AND source IN ({placeholders}) "
            "ORDER BY source"
        )
        rows = self._conn.execute(query, sources).fetchall()
        return [_row_to_grant(row) for row in rows]

    def get_stale_grants(self, new_hashes: dict[tuple[str, str], str]) -> list[Grant]:
        """Return grants whose raw_text has changed since last enrichment."""
        all_enriched = self._conn.execute(
            "SELECT * FROM grants WHERE enriched = 1 AND raw_text != ''"
        ).fetchall()
        stale = []
        for row in all_enriched:
            key = (row["source"], row["source_id"])
            new_hash = new_hashes.get(key)
            if new_hash and new_hash != row["raw_text_hash"]:
                stale.append(_row_to_grant(row))
        return stale

    def get_ckan_grants_without_url(self) -> list[Grant]:
        """Return CKAN grants that have no url set (for URL resolution from ESDC/BF)."""
        rows = self._conn.execute(
            "SELECT * FROM grants WHERE source = 'ckan' AND (url = '' OR url IS NULL)"
        ).fetchall()
        return [_row_to_grant(row) for row in rows]

    def get_grants_for_detail_refresh(
        self, *, sources: tuple[str, ...] = ("benefits-finder", "ckan", "esdc")
    ) -> list[Grant]:
        """Return grants that have a URL and belong to one of the given sources (for refresh-details)."""
        if not sources:
            return []
        placeholders = ",".join("?" * len(sources))
        query = (
            f"SELECT * FROM grants WHERE url != '' AND url IS NOT NULL AND source IN ({placeholders}) "
            "ORDER BY source, source_id"
        )
        rows = self._conn.execute(query, sources).fetchall()
        return [_row_to_grant(row) for row in rows]

    # ------------------------------------------------------------------
    # Web app queries
    # ------------------------------------------------------------------

    def get_grant_by_id(self, grant_id: int) -> Grant | None:
        row = self._conn.execute("SELECT * FROM grants WHERE id = ?", (grant_id,)).fetchone()
        return _row_to_grant(row) if row else None

    def get_stats(self) -> dict:
        row = self._conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(accepting_applications) as accepting,
                SUM(CASE WHEN funding_min IS NOT NULL OR funding_max IS NOT NULL THEN 1 ELSE 0 END) as has_funding
            FROM grants
        """).fetchone()
        return {"total": row[0], "accepting": row[1] or 0, "has_funding": row[2] or 0}

    def get_all_categories(self) -> list[str]:
        rows = self._conn.execute("SELECT DISTINCT categories FROM grants WHERE categories != '[]'").fetchall()
        cats: set[str] = set()
        for row in rows:
            for c in json.loads(row[0]):
                cats.add(c)
        return sorted(cats)

    def get_all_provinces(self) -> list[str]:
        rows = self._conn.execute("SELECT DISTINCT provinces FROM grants WHERE provinces != '[]'").fetchall()
        provs: set[str] = set()
        for row in rows:
            for p in json.loads(row[0]):
                provs.add(p)
        return sorted(provs)

    def get_all_organization_types(self) -> list[str]:
        rows = self._conn.execute("SELECT DISTINCT organization_types FROM grants WHERE organization_types != '[]'").fetchall()
        org_types: set[str] = set()
        for row in rows:
            for o in json.loads(row[0]):
                org_types.add(o)
        return sorted(org_types)

    def search_grants_filtered(
        self,
        *,
        keyword: str = "",
        categories: list[str] | None = None,
        provinces: list[str] | None = None,
        organization_types: list[str] | None = None,
        accepting_only: bool = False,
        funding_min: int | None = None,
        funding_max: int | None = None,
        sort: str = "relevance",
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[Grant], int]:
        """Faceted search returning (grants, total_count)."""
        where_clauses = ["1=1"]
        params: list = []

        if keyword:
            where_clauses.append(
                "(LOWER(title) LIKE ? OR LOWER(description) LIKE ? "
                "OR LOWER(organization) LIKE ? OR LOWER(eligibility) LIKE ?)"
            )
            term = f"%{keyword.lower()}%"
            params.extend([term, term, term, term])

        if accepting_only:
            where_clauses.append("accepting_applications = 1")

        if funding_min is not None:
            where_clauses.append("(funding_max IS NULL OR funding_max >= ?)")
            params.append(funding_min)

        if funding_max is not None:
            where_clauses.append("(funding_min IS NULL OR funding_min <= ?)")
            params.append(funding_max)

        if categories:
            cat_conditions = []
            for cat in categories:
                cat_conditions.append("categories LIKE ?")
                params.append(f'%"{cat}"%')
            where_clauses.append(f"({' OR '.join(cat_conditions)})")

        if provinces:
            prov_conditions = []
            for prov in provinces:
                prov_conditions.append("(provinces LIKE ? OR provinces LIKE ?)")
                params.append(f'%"{prov}"%')
                params.append(f'%"ALL"%')
            where_clauses.append(f"({' OR '.join(prov_conditions)})")

        if organization_types:
            org_conditions = []
            for org in organization_types:
                org_conditions.append("organization_types LIKE ?")
                params.append(f'%"{org}"%')
            where_clauses.append(f"({' OR '.join(org_conditions)})")

        where = " AND ".join(where_clauses)

        count_row = self._conn.execute(f"SELECT COUNT(*) FROM grants WHERE {where}", params).fetchone()
        total = count_row[0]

        sort_clause = {
            "relevance": "relevance_score DESC, last_crawled DESC",
            "funding_desc": "COALESCE(funding_max, 0) DESC, relevance_score DESC",
            "funding_asc": "COALESCE(funding_min, 999999999) ASC, relevance_score DESC",
            "deadline": "CASE WHEN deadline IS NULL OR deadline = '' THEN 1 ELSE 0 END, deadline ASC",
            "newest": "last_crawled DESC",
        }.get(sort, "relevance_score DESC, last_crawled DESC")

        query = f"SELECT * FROM grants WHERE {where} ORDER BY {sort_clause} LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        rows = self._conn.execute(query, params).fetchall()
        return [_row_to_grant(row) for row in rows], total

    def get_similar_grants(self, grant_id: int, limit: int = 6) -> list[Grant]:
        grant = self.get_grant_by_id(grant_id)
        if not grant or not grant.categories:
            return []
        cat_conditions = []
        params: list = []
        for cat in grant.categories:
            cat_conditions.append("categories LIKE ?")
            params.append(f'%"{cat}"%')
        where = f"({' OR '.join(cat_conditions)}) AND id != ?"
        params.append(grant_id)
        query = f"SELECT * FROM grants WHERE {where} ORDER BY relevance_score DESC LIMIT ?"
        params.append(limit)
        rows = self._conn.execute(query, params).fetchall()
        return [_row_to_grant(row) for row in rows]

    def get_featured_grants(self, limit: int = 6) -> list[Grant]:
        rows = self._conn.execute(
            "SELECT * FROM grants WHERE accepting_applications = 1 "
            "ORDER BY relevance_score DESC, last_crawled DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [_row_to_grant(row) for row in rows]


def _row_to_grant(row: sqlite3.Row) -> Grant:
    return Grant(
        id=row["id"],
        title=row["title"],
        organization=row["organization"],
        url=row["url"],
        description=row["description"],
        deadline=row["deadline"],
        funding_min=row["funding_min"],
        funding_max=row["funding_max"],
        eligibility=row["eligibility"],
        funding_level=FundingLevel(row["funding_level"]),
        contact_info=row["contact_info"],
        source=row["source"],
        source_id=row["source_id"],
        status=row["status"],
        last_crawled=datetime.fromisoformat(row["last_crawled"]),
        raw_text=row["raw_text"],
        raw_text_hash=row["raw_text_hash"],
        enriched=bool(row["enriched"]),
        relevance_score=row["relevance_score"],
        accepting_applications=bool(row["accepting_applications"]),
        is_applyable_grant=bool(row["is_applyable_grant"]),
        categories=json.loads(row["categories"]) if row["categories"] else [],
        provinces=json.loads(row["provinces"]) if row["provinces"] else [],
        organization_types=json.loads(row["organization_types"]) if row["organization_types"] else [],
    )
