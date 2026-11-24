"""Domain normalization shared by the log reader and API validators."""

from __future__ import annotations


def normalize_domain(value: str) -> str | None:
    """Canonicalize a host field.

    Domains are grouped case-insensitively and a trailing root dot is ignored.
    Returns ``None`` for values that cannot be a sane host token.
    """

    domain = value.strip().lower().rstrip(".")
    if not domain or len(domain) > 255:
        return None
    if any(ch.isspace() or ord(ch) < 32 or ord(ch) == 127 for ch in domain):
        return None
    return domain
