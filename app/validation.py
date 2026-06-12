"""Validate source records without mutating the original state vector."""
import math
import re
from datetime import datetime, timezone

FIELDS = ("icao24", "callsign", "origin_country", "time_position", "last_contact",
          "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
          "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
          "spi", "position_source", "category")


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def validate(state, now=None, retention_days=14):
    if not isinstance(state, list) or len(state) not in (17, 18):
        raise ValueError("invalid_state_vector_shape")
    # PostgreSQL JSONB cannot store NULs or unpaired Unicode surrogates.
    def check_json_text(value):
        if isinstance(value, str):
            if '\x00' in value:
                raise ValueError('invalid_text_encoding')
            try:
                value.encode('utf-8')
            except UnicodeEncodeError:
                raise ValueError('invalid_text_encoding') from None
        elif isinstance(value, list):
            for item in value:
                check_json_text(item)
        elif isinstance(value, dict):
            for key, item in value.items():
                check_json_text(key)
                check_json_text(item)
    check_json_text(state)
    row = dict(zip(FIELDS, state))
    if not isinstance(row["icao24"], str) or not re.fullmatch(r"[0-9a-f]{6}", row["icao24"]):
        raise ValueError("invalid_icao24")
    now = now or datetime.now(timezone.utc)
    stamp = row["time_position"]
    if not number(stamp) or int(stamp) != stamp:
        raise ValueError("missing_or_invalid_time_position")
    if not now.timestamp() - retention_days * 86400 <= stamp <= now.timestamp() + 300:
        raise ValueError("time_position_outside_retention_or_future")
    for field, low, high in (("latitude", -90, 90), ("longitude", -180, 180)):
        if not number(row[field]) or not low <= row[field] <= high:
            raise ValueError("invalid_" + field)
    for field, low, high in (("baro_altitude", -500, 20000), ("velocity", 0, 350)):
        if row[field] is not None and (not number(row[field]) or not low <= row[field] <= high):
            raise ValueError("invalid_" + field)
    if not isinstance(row["on_ground"], bool):
        raise ValueError("invalid_on_ground")
    for field in ("callsign", "origin_country"):
        if row[field] is not None and not isinstance(row[field], str):
            raise ValueError("invalid_" + field)
    row["position_at"] = datetime.fromtimestamp(stamp, timezone.utc)
    return row
