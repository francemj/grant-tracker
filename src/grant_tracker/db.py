from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

from grant_tracker.models import FundingLevel, Grant

DEFAULT_DB_PATH = Path("grants.db")

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
    UNIQUE(source, source_id)
);
"""

MIGRATIONS = [
    "ALTER TABLE grants ADD COLUMN raw_text TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE grants ADD COLUMN raw_text_hash TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE grants ADD COLUMN enriched INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE grants ADD COLUMN relevance_score REAL NOT NULL DEFAULT 0.0",
    "ALTER TABLE grants ADD COLUMN accepting_applications INTEGER NOT NULL DEFAULT 0",
]


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

    def upsert_grant(self, grant: Grant) -> None:
        self._conn.execute(
            """
            INSERT INTO grants (
                title, organization, url, description, deadline,
                funding_min, funding_max, eligibility, funding_level,
                contact_info, source, source_id, status, last_crawled,
                raw_text, raw_text_hash, enriched, relevance_score, accepting_applications
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                accepting_applications = excluded.accepting_applications
            """,
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
            ),
        )
        self._conn.commit()

    def upsert_many(self, grants: list[Grant]) -> int:
        count = 0
        for grant in grants:
            self.upsert_grant(grant)
            count += 1
        return count

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


def _row_to_grant(row: sqlite3.Row) -> Grant:
    return Grant(
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
    )
