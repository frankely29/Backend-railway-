"""Driver-facing phrasing for the guidance directive.

The decision (move / hold / where) is made in the engine; this turns it into a
line that reads like a sharp dispatcher instead of a template: lead with the
call, state the demand level and which way it's trending (in the map's own
colour words), name the spot the way a driver would say it ("the Broadway
stop", not "Broadway, the local transit hub"), and contrast against the
current zone on a move. Pure/string-only so it can be unit-tested offline.
"""

import re
from typing import Any, Mapping, Optional

# Rating -> demand colour, matching the frontend's colorFromRating buckets so
# the words line up with what the driver sees on the map.
def bucket_word(rating: float) -> str:
    r = float(rating or 0)
    if r >= 83:
        return "green"
    if r >= 75:
        return "purple"
    if r >= 68:
        return "indigo"
    if r >= 60:
        return "blue"
    if r >= 50:
        return "sky blue"
    if r >= 40:
        return "yellow"
    if r >= 30:
        return "orange"
    return "red"


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
        return f"the pickup cluster around {label}"
    if source == "curated":
        address = str(spot.get("address") or "").strip()
        return f"{label} ({address})" if address else label
    # OSM structural magnet
    if spot.get("kind") == "rail":
        low = label.lower()
        if low.endswith(("station", "terminal", "stop")):
            return label
        return f"the {label} stop"
    return label  # hospital / mall / university / attraction / transit_minor


def _busy_tag(spot: Optional[Mapping[str, Any]]) -> str:
    return " Busy right now." if spot and spot.get("prime_now") else ""


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
    czone = current_zone_name or "this zone"
    cur_word = bucket_word(current_rating)
    sp = spot_phrase(spot)

    # --- Move to a different zone -----------------------------------------
    if moving and target_zone_name:
        tgt_word = bucket_word(target_rating)
        eta_txt = f" ~{int(round(float(target_eta)))} min." if target_eta else ""
        climb_txt = " and still climbing" if _trend(target_rating_now, target_rating) == "climbing" else ""
        spot_txt = f" {_cap(sp)} is the spot there." if sp else ""
        if tgt_word != cur_word:
            head = f"Head to {target_zone_name} — {tgt_word} there{climb_txt} vs {cur_word} here."
        else:
            head = f"Head to {target_zone_name} — {tgt_word}{climb_txt} and stronger than here."
        return (head + spot_txt + eta_txt).strip()

    # --- Reposition within the same zone ----------------------------------
    if action == "micro_reposition":
        if sp:
            return f"Shift spots in {czone} — it's gone quiet. Try {sp}."
        return f"Shift to a busier corner of {czone} — your spot's gone quiet."

    # --- Hold a blue+ zone -------------------------------------------------
    if not below_blue:
        trend = _trend(current_rating, current_next_rating)
        anchor = f" Work {sp}." if sp else ""
        if trend == "climbing":
            return (f"Stay put in {czone} — {cur_word} and still building.{anchor}{_busy_tag(spot)}").rstrip()
        if trend == "cooling":
            tail = f" {_cap(sp)} while it lasts." if sp else ""
            return (f"Hold in {czone} — {cur_word} but easing.{tail}").rstrip()
        return (f"Stay in {czone} — {cur_word} and steady.{anchor}{_busy_tag(spot)}").rstrip()

    # --- Below blue but about to climb (held for the rise) ----------------
    if current_will_improve:
        if sp:
            return f"Sit tight in {czone} — work {sp} while it builds."
        return f"Sit tight in {czone} — it's about to build."

    # --- Below blue, not improving (nothing better / let dispatch work) ---
    if sp:
        return f"Hold in {czone} a few minutes — work {sp} while dispatch catches up."
    return f"Hold in {czone} — give dispatch a few minutes."
