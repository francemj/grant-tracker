"""Resolve URLs for CKAN-sourced grants by matching to ESDC and Benefits Finder.

The Proactive Disclosure API has no program URL field. This module builds
lookups from grants that have URLs (ESDC, benefits-finder) and assigns
url to CKAN grants when the normalized title matches.
"""

from __future__ import annotations

from grant_tracker.models import Grant


def normalize_title(title: str) -> str:
    """Lowercase, collapse whitespace to single space, strip. Used for matching."""
    if not title:
        return ""
    return " ".join(title.lower().strip().split())


def build_url_lookup(grants: list[Grant], *, source: str) -> dict[str, str]:
    """Build normalize_title(title) -> url for grants with the given source and non-empty url.

    If duplicate normalized titles exist, the first url is kept.
    """
    out: dict[str, str] = {}
    for g in grants:
        if g.source != source or not (g.url and g.url.strip()):
            continue
        key = normalize_title(g.title)
        if key and key not in out:
            out[key] = g.url.strip()
    return out


# Substring to restrict ESDC lookup to CKAN programs from the same department.
ESDC_ORG_SUBSTRING = "Employment and Social Development"


def resolve_ckan_urls(
    ckan_grants: list[Grant],
    esdc_lookup: dict[str, str],
    bf_lookup: dict[str, str],
    *,
    esdc_org_filter: bool = True,
) -> list[Grant]:
    """Set url on CKAN grants when normalized title matches ESDC or Benefits Finder.

    Tries ESDC first (optionally only when CKAN organization contains ESDC_ORG_SUBSTRING),
    then Benefits Finder. Mutates grants in place. Returns the list of grants that had
    their url set (for upsert).
    """
    updated: list[Grant] = []
    for g in ckan_grants:
        if not g.url or not g.url.strip():
            key = normalize_title(g.title)
            if not key:
                continue
            url = None
            if not esdc_org_filter or ESDC_ORG_SUBSTRING in (g.organization or ""):
                url = esdc_lookup.get(key)
            if url is None:
                url = bf_lookup.get(key)
            if url:
                g.url = url
                updated.append(g)
    return updated
