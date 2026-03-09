from __future__ import annotations

import hashlib
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class FundingLevel(str, Enum):
    FEDERAL = "federal"
    PROVINCIAL = "provincial"
    MUNICIPAL = "municipal"
    PRIVATE = "private"


class EnrichedFields(BaseModel):
    """Schema sent to the LLM for structured extraction."""

    title: str = Field(description="Clean, concise program title")
    description: str = Field(description="1-3 sentence summary of what the grant funds")
    funding_min: int | None = Field(description="Minimum award amount in CAD dollars. null if unclear or not specified.")
    funding_max: int | None = Field(description="Maximum award amount in CAD dollars. null if unclear or not specified.")
    eligibility: str | None = Field(description="Who can apply. Concise bullet-point list. null if not found.")
    deadline: str | None = Field(description="Application deadline as a human-readable string. null if ongoing, continuous intake, or not found.")
    contact_info: str | None = Field(description="Contact email address or phone number. null if not found.")
    funding_level: FundingLevel = Field(description="Level of government providing the grant")
    relevance_score: float = Field(description="0.0 to 1.0 — how relevant is this grant for Canadian not-for-profit organizations")
    accepting_applications: bool = Field(description="True if the program is currently accepting applications (open deadline, continuous intake, or explicitly stated as accepting). False if closed, not currently accepting, or unclear.")


class Grant(BaseModel):
    title: str
    organization: str
    url: str = ""
    description: str = ""
    deadline: str | None = None
    funding_min: int | None = None
    funding_max: int | None = None
    eligibility: str | None = None
    funding_level: FundingLevel = FundingLevel.FEDERAL
    contact_info: str | None = None

    source: str = Field(description="Crawler that produced this record (esdc, ckan, benefits-finder)")
    source_id: str = Field(description="Unique ID within the source for deduplication")
    status: str | None = Field(default=None, description="e.g. 'Accepting applications'")
    last_crawled: datetime = Field(default_factory=datetime.utcnow)

    raw_text: str = Field(default="", description="Full scraped text used for LLM enrichment")
    raw_text_hash: str = Field(default="", description="SHA-256 of raw_text for change detection")
    enriched: bool = Field(default=False, description="Whether LLM enrichment has been applied")
    relevance_score: float = Field(default=0.0, description="LLM-assigned relevance to non-profits (0-1)")
    accepting_applications: bool = Field(default=False, description="Whether the program is currently accepting applications")

    def compute_raw_text_hash(self) -> str:
        return hashlib.sha256(self.raw_text.encode()).hexdigest()[:16]
