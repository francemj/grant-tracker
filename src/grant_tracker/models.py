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


CATEGORY_TAXONOMY = [
    "arts-culture",
    "agriculture",
    "business-industry",
    "community-development",
    "education-training",
    "employment",
    "environment-climate",
    "health",
    "housing",
    "indigenous",
    "infrastructure",
    "international",
    "justice-safety",
    "research-innovation",
    "science-technology",
    "social-services",
    "sport-recreation",
    "tourism",
    "transportation",
    "youth",
]

ORGANIZATION_TYPE_TAXONOMY = [
    "non-profit",
    "indigenous-org",
    "municipality",
    "province-territory",
    "academic-institution",
    "small-business",
    "industry",
    "individual",
    "other",
]

PROVINCE_CODES = [
    "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT", "ALL",
]


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
    is_applyable_grant: bool = Field(
        description=(
            "True if this is an applyable funding opportunity (a grant/contribution with an external application process). "
            "False if it is primarily a service, advisor, tool, informational page, certification, or other non-funding offering."
        )
    )
    categories: list[str] = Field(
        description=(
            "1-3 category tags from this fixed list: "
            + ", ".join(CATEGORY_TAXONOMY)
            + ". Pick the most specific matches."
        )
    )
    provinces: list[str] = Field(
        description=(
            "Canadian province/territory codes where applicants must be located, from: "
            + ", ".join(PROVINCE_CODES)
            + '. Use "ALL" if the grant is open nationally or no geographic restriction is stated.'
        )
    )
    organization_types: list[str] = Field(
        description=(
            "Types of organizations eligible to apply, from: "
            + ", ".join(ORGANIZATION_TYPE_TAXONOMY)
            + ". Pick all that apply."
        )
    )


class Grant(BaseModel):
    id: int | None = Field(default=None, description="Auto-assigned database row ID")
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
    is_applyable_grant: bool = Field(default=True, description="Whether this row is an applyable grant (vs a service/tool)")

    categories: list[str] = Field(default_factory=list, description="Category tags for discovery")
    provinces: list[str] = Field(default_factory=list, description="Province/territory eligibility codes")
    organization_types: list[str] = Field(default_factory=list, description="Eligible organization types")

    def compute_raw_text_hash(self) -> str:
        return hashlib.sha256(self.raw_text.encode()).hexdigest()[:16]
