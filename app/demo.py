"""Deterministic synthetic aircraft; no real registrations or measurements."""
import math


def snapshot(timestamp):
    states = []
    for i in range(32):
        phase = timestamp / 1000 + i
        states.append([f"{0xf00000+i:06x}", f"DEMO{i:03d} ", "Synthetic",
            timestamp, timestamp, -4.5 + 1.8 * math.sin(phase),
            35 + 1.5 * math.cos(phase), 7000 + i * 100, False,
            180 + i, (phase * 30) % 360, 0, None, 7200 + i * 100,
            "0000", False, 0])
    return {"time": timestamp, "states": states}
