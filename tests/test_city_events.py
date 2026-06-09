"""Tests for the city_events feature (Ticketmaster -> map).

Runs fully offline: `normalize_event` is pure, the DB path uses the
conftest SQLite tmpdir, and the fetch path is exercised only in its
unconfigured (no API key) form.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

import city_events as ce
from core import _db_exec


# Realistic Ticketmaster Discovery API event shapes (trimmed to fields used).
SAMPLE_CONCERT = {
    "id": "vvG1zZ-concert",
    "name": "Some Band — Live at the Garden",
    "url": "https://www.ticketmaster.com/event/vvG1zZ-concert",
    "dates": {"start": {"localDate": "2026-06-08", "localTime": "20:00:00",
                        "dateTime": "2026-06-09T00:00:00Z"}},
    "classifications": [{"segment": {"id": "KZFzniwnSyZfZ7v7nJ", "name": "Music"}}],
    "_embedded": {"venues": [{"name": "Madison Square Garden",
                              "location": {"latitude": "40.7505", "longitude": "-73.9934"}}]},
}

SAMPLE_SPORTS = {
    "id": "vvG1zZ-sports",
    "name": "Yankees vs Red Sox",
    "url": "https://www.ticketmaster.com/event/vvG1zZ-sports",
    "dates": {"start": {"localDate": "2026-06-08", "localTime": "19:05:00",
                        "dateTime": "2026-06-08T23:05:00Z"}},
    "classifications": [{"segment": {"id": "KZFzniwnSyZfZ7v7nE", "name": "Sports"}}],
    "_embedded": {"venues": [{"name": "Yankee Stadium",
                              "location": {"latitude": "40.8296", "longitude": "-73.9262"}}]},
}

SAMPLE_THEATRE = {  # Arts & Theatre segment -> not one of our 3 -> skipped
    "id": "vvG1zZ-theatre",
    "name": "A Broadway Show",
    "dates": {"start": {"dateTime": "2026-06-08T23:00:00Z"}},
    "classifications": [{"segment": {"id": "KZFzniwnSyZfZ7v7na", "name": "Arts & Theatre"}}],
    "_embedded": {"venues": [{"name": "Some Theatre",
                              "location": {"latitude": "40.76", "longitude": "-73.98"}}]},
}

SAMPLE_NO_COORDS = {  # Music but venue has no location and is unknown -> skipped
    "id": "vvG1zZ-nocoords",
    "name": "Festival TBA",
    "dates": {"start": {"dateTime": "2026-06-08T23:00:00Z"}},
    "classifications": [{"segment": {"id": "KZFzniwnSyZfZ7v7nJ", "name": "Music"}}],
    "_embedded": {"venues": [{"name": "Unknown Pop-up Lot"}]},
}

SAMPLE_FALLBACK_COORDS = {  # no location, but venue name matches the fallback table
    "id": "vvG1zZ-fallback",
    "name": "Concert at Barclays",
    "dates": {"start": {"dateTime": "2026-06-08T23:00:00Z"}},
    "classifications": [{"segment": {"id": "KZFzniwnSyZfZ7v7nJ", "name": "Music"}}],
    "_embedded": {"venues": [{"name": "Barclays Center"}]},
}


def test_normalize_concert_and_sports():
    c = ce.normalize_event(SAMPLE_CONCERT)
    assert c is not None
    assert c["category"] == "concert"
    assert c["name"] == "Some Band — Live at the Garden"
    assert c["venue"] == "Madison Square Garden"
    assert abs(c["lat"] - 40.7505) < 1e-6 and abs(c["lng"] - (-73.9934)) < 1e-6
    assert c["source"] == "ticketmaster" and c["source_id"] == "vvG1zZ-concert"
    # dateTime "2026-06-09T00:00:00Z" -> exact unix
    expected = int(datetime(2026, 6, 9, 0, 0, 0, tzinfo=timezone.utc).timestamp())
    assert c["start_at"] == expected

    s = ce.normalize_event(SAMPLE_SPORTS)
    assert s is not None and s["category"] == "sports" and s["venue"] == "Yankee Stadium"


def test_normalize_skips_unwanted_or_coordless():
    assert ce.normalize_event(SAMPLE_THEATRE) is None       # segment not requested
    assert ce.normalize_event(SAMPLE_NO_COORDS) is None      # no coordinates
    assert ce.normalize_event({}) is None                    # garbage
    assert ce.normalize_event({"id": "x"}) is None           # no name


def test_normalize_venue_coordinate_fallback():
    e = ce.normalize_event(SAMPLE_FALLBACK_COORDS)
    assert e is not None
    assert abs(e["lat"] - 40.6826) < 1e-6 and abs(e["lng"] - (-73.9754)) < 1e-6


def test_parse_start_localdate_fallback():
    # No dateTime -> localDate + localTime interpreted as NYC.
    ts = ce._parse_start_unix({"localDate": "2026-06-08", "localTime": "20:00:00"})
    assert ts is not None
    assert datetime.fromtimestamp(ts, ce.NYC_TZ).hour == 20


def test_fetch_returns_empty_without_key():
    # The test env has no TICKETMASTER_API_KEY, so the fetcher is dormant.
    assert ce.events_api_is_configured() is False
    assert ce.fetch_nyc_events_today() == []


def test_schema_upsert_select_roundtrip():
    ce.ensure_city_events_schema()
    _db_exec("DELETE FROM city_events")  # isolate this test
    now = int(time.time())
    rows = [
        {**ce.normalize_event(SAMPLE_CONCERT), "start_at": now - 600},   # 10 min ago
        {**ce.normalize_event(SAMPLE_SPORTS), "start_at": now - 1200},   # 20 min ago
    ]
    stored = ce.upsert_events(rows)
    assert stored == 2

    out = ce.select_events_for_today()
    names = {e["name"] for e in out["events"]}
    assert "Some Band — Live at the Garden" in names
    assert "Yankees vs Red Sox" in names
    one = next(e for e in out["events"] if e["category"] == "concert")
    assert set(one.keys()) >= {"id", "name", "category", "venue", "lat", "lng", "startAt", "endAt", "url"}
    assert one["startAt"] == now - 600 and one["endAt"] is None

    # Idempotent upsert: same source ids -> updated in place, still 2 rows.
    ce.upsert_events(rows)
    assert len(ce.select_events_for_today()["events"]) == 2


def test_prune_old_events():
    ce.ensure_city_events_schema()
    _db_exec("DELETE FROM city_events")
    now = int(time.time())
    ce.upsert_events([
        {**ce.normalize_event(SAMPLE_CONCERT), "source_id": "old-1",
         "start_at": now - ce.MAX_EVENT_SPAN_SECONDS - 3600},  # well past
        {**ce.normalize_event(SAMPLE_SPORTS), "source_id": "fresh-1", "start_at": now - 300},
    ])
    ce.prune_old_events()
    names = {e["name"] for e in ce.select_events_for_today()["events"]}
    assert "Yankees vs Red Sox" in names           # fresh kept
    assert "Some Band — Live at the Garden" not in names  # old pruned
