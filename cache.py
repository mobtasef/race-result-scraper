from __future__ import annotations

import json
import os
import threading

CACHE_FILE = os.path.join(os.path.dirname(__file__), "race_cache.json")
MAX_ENTRIES = 5
_lock = threading.Lock()


def _read() -> list:
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def _write(entries: list) -> None:
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False)


def save(entry: dict) -> None:
    """Prepend entry, deduplicate by race_id, keep MAX_ENTRIES."""
    with _lock:
        entries = _read()
        entries = [e for e in entries if e.get("race_id") != entry["race_id"]]
        entries.insert(0, entry)
        _write(entries[:MAX_ENTRIES])


def list_events() -> list:
    """Return metadata only (no full results) for the sidebar."""
    with _lock:
        entries = _read()
    return [
        {
            "race_id":    e["race_id"],
            "race_name":  e["race_name"],
            "url":        e["url"],
            "scraped_at": e["scraped_at"],
            "count":      e["count"],
            "categories": e.get("categories", []),
        }
        for e in entries
    ]


def get(race_id: str) -> dict | None:
    with _lock:
        for e in _read():
            if e["race_id"] == race_id:
                return e
    return None
