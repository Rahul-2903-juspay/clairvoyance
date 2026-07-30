"""Registry for three-operation Human Assist platform adapters."""

from __future__ import annotations

import re
from typing import Dict, List

from app.services.human_assist.platforms.base import (
    HumanAssistPlatform,
    HumanAssistPlatformError,
    HumanAssistPlatformSpec,
)

_PLATFORM_KEY = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_PLATFORMS: Dict[str, HumanAssistPlatform] = {}


def register_platform(adapter: HumanAssistPlatform) -> None:
    """Register one adapter; duplicate or malformed keys fail at startup."""
    key = adapter.spec.key
    if not _PLATFORM_KEY.fullmatch(key):
        raise ValueError(f"Invalid Human Assist platform key: {key!r}")
    if key in _PLATFORMS:
        raise ValueError(f"Duplicate Human Assist platform key: {key!r}")
    _PLATFORMS[key] = adapter


def get_platform(key: str) -> HumanAssistPlatform:
    adapter = _PLATFORMS.get(key)
    if adapter is None:
        raise HumanAssistPlatformError(
            f"Human Assist platform '{key}' is not registered."
        )
    return adapter


def is_platform_registered(key: str) -> bool:
    return key in _PLATFORMS


def list_platforms() -> List[HumanAssistPlatformSpec]:
    return [adapter.spec for adapter in _PLATFORMS.values()]


__all__ = [
    "get_platform",
    "is_platform_registered",
    "list_platforms",
    "register_platform",
]
