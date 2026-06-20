"""Driver-facing phrasing for the guidance directive.

The engine decides move/hold/where; this turns it into one plain, confident
line a driver can read at a glance: lead with STAY or GO, say how busy it is
and which way it's heading in plain words, name the spot the way a driver
would say it, and on a move give the payoff and the ETA. No jargon, no
homework, nothing left out. Pure/string-only so it can be unit-tested.
"""

import re
from typing import Any, Mapping, Optional


def _rank(rating: float) -> int:
    """Coarse busy-ness rank, for comparing two zones."""
    r = float(rating or 0)
    if r >= 75:
        return 5
    if r >= 68:
        return 4
    if r >= 60:
        return 3
    if r >= 50:
        return 2
    if r >= 40:
        return 1
    return 0


def demand_word(rating: float) -> str:
    """Plain busy-ness word (no colour/jargon)."""
    r = float(rating or 0)
    if r >= 75:
        return "red-hot"
    if r >= 68:
        return "very busy"
    if r >= 60:
        return "busy"
    if r >= 50:
        return "lukewarm"
    if r >= 40:
        return "slow"
    return "quiet"


def _trend(rating: float, next_rating: Optional[float]) -> str:
    r = float(rating or 0)
    n = float(next_rating if next_rating is not None else r)
    if n >= r + 4:
        return "climbing"
    if n <= r - 4:
        return "cooling"
    return "steady"


def _clean_label(label: Any) -> str:
    # "Boro Hotel LIC +3" -> "Boro Hotel LIC" (the +N is cluster metadata).
    return re.sub(r"\s*\+\d+\s*$", "", str(label or "")).strip()


def _cap(text: str) -> str:
    return (text[:1].upper() + text[1:]) if text else text


def spot_phrase(spot: Optional[Mapping[str, Any]]) -> Optional[str]:
    """How a driver would refer to the spot, by source/kind."""
    if not spot:
        return None
    label = _clean_label(spot.get("label"))
    if not label:
        return None
    source = spot.get("source")
    if source == "pickup":
        return f"the pickup cluster at {label}"
    if source == "curated":
        address = str(spot.get("address") or "").strip()
        return f"{label} ({address})" if address else label
    if spot.get("kind") == "rail":
        low = label.lower()
        if low.endswith(("station", "terminal", "stop", "hall", "concourse")):
            return label
        return f"the {label} stop"
    return label  # hospital / mall / university / attraction / transit_minor


def compose_guidance_directive(
    *,
    action: str,
    moving: bool,
    current_zone_name: Optional[str],
    current_rating: float,
    current_next_rating: float,
    target_zone_name: Optional[str] = None,
    target_rating: float = 0.0,
    target_rating_now: float = 0.0,
    target_eta: float = 0.0,
    spot: Optional[Mapping[str, Any]] = None,
    below_blue: bool = False,
    current_will_improve: bool = False,
) -> str:
    czone = current_zone_name or "this area"
    sp = spot_phrase(spot)

    # --- GO: move to a different zone --------------------------------------
    if moving and target_zone_name:
        gap = _rank(target_rating) - _rank(current_rating)
        if gap >= 2:
            lead = "much busier than here"
        elif gap == 1:
            lead = "busier than here"
        else:
            lead = f"{demand_word(target_rating)} there"
        if _trend(target_rating_now, target_rating) == "climbing":
            lead += " and still climbing"
        spot_txt = f" Set up at {sp}." if sp else ""
        eta_txt = f" ~{int(round(float(target_eta)))} min away." if target_eta else ""
        return f"Go to {target_zone_name} — {lead}.{spot_txt}{eta_txt}".strip()

    # --- Reposition within the same zone -----------------------------------
    if action == "micro_reposition":
        if sp:
            return f"Move to a busier corner of {czone} — try {sp}."
        return f"Move to a busier corner of {czone}."

    # --- STAY: busy zone (blue+) -------------------------------------------
    if not below_blue:
        word = demand_word(current_rating)
        trend = _trend(current_rating, current_next_rating)
        busy_now = " It's busy right now." if (spot and spot.get("prime_now")) else ""
        if trend == "climbing":
            tail = f" Work {sp}." if sp else ""
            return f"Stay in {czone} — {word} and getting busier.{tail}{busy_now}".rstrip()
        if trend == "cooling":
            tail = f" Work {sp}." if sp else ""
            return f"Stay in {czone} — {word} but slowing down.{tail}".rstrip()
        tail = f" Work {sp}." if sp else ""
        return f"Stay in {czone} — {word} and steady.{tail}{busy_now}".rstrip()

    # --- STAY: below blue but about to pick up -----------------------------
    if current_will_improve:
        if sp:
            return f"Stay in {czone} — it's about to pick up. Work {sp}."
        return f"Stay in {czone} — it's about to pick up."

    # --- STAY: below blue, nothing reachable is better yet -----------------
    if sp:
        return f"Stay in {czone} for now — nothing nearby is better. Work {sp}."
    return f"Stay in {czone} for now — nothing nearby is better."
