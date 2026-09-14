from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Iterator, Optional


RAW_EVENTS = [
    {"timestamp": "08:00", "route": "AITU-Campus-Residence", "bus": "B01", "passengers": 18, "speed_kmh": 31, "status": "ON_ROUTE"},
    {"timestamp": "08:01", "route": "AITU-Campus-Residence", "bus": "B02", "passengers": 22, "speed_kmh": 28, "status": "ON_ROUTE"},
    {"timestamp": "08:02", "route": "AITU-Campus-Residence", "bus": "B01", "passengers": 21, "speed_kmh": 29, "status": "ON_ROUTE"},
    {"timestamp": "08:03", "route": "AITU-Campus-Residence", "bus": "B02", "passengers": 25, "speed_kmh": 27, "status": "ON_ROUTE"},
    {"timestamp": "08:04", "route": "AITU-Campus-Residence", "bus": "B01", "passengers": 24, "speed_kmh": 0, "status": "STOPPED"},
    {"timestamp": "08:05", "route": "AITU-Campus-Residence", "bus": "B02", "passengers": 26, "speed_kmh": 30, "status": "ON_ROUTE"},
    {"timestamp": "08:06", "route": "AITU-Campus-Residence", "bus": "B01", "passengers": 23, "speed_kmh": 32, "status": "ON_ROUTE"},
    {"timestamp": "08:07", "route": "AITU-Campus-Residence", "bus": "B02", "passengers": 28, "speed_kmh": 26, "status": "ON_ROUTE"},
    {"timestamp": "08:08", "route": "AITU-Campus-Residence", "bus": "B01", "passengers": 27, "speed_kmh": 25, "status": "ON_ROUTE"},
    {"timestamp": "08:09", "route": "AITU-Campus-Residence", "bus": "B02", "passengers": 30, "speed_kmh": 24, "status": "ON_ROUTE"},
]

VALID_STATUSES = {"ON_ROUTE", "STOPPED"}
MIN_SPEED, MAX_SPEED = 0, 120


class EventValidationError(Exception):
    pass


@dataclass
class ShuttleEvent:
    timestamp: "datetime.time"
    route: str
    bus: str
    passengers: int
    speed_kmh: int
    status: str

    def to_dict(self) -> dict:
        d = asdict(self)
        d["timestamp"] = self.timestamp.strftime("%H:%M")
        return d


def parse_timestamp(raw_timestamp: str):
    try:
        return datetime.strptime(raw_timestamp.strip(), "%H:%M").time()
    except (ValueError, AttributeError) as exc:
        raise EventValidationError(f"Invalid timestamp '{raw_timestamp}': {exc}") from exc


def validate_event(raw_event: dict) -> ShuttleEvent:
    required_fields = ("timestamp", "route", "bus", "passengers", "speed_kmh", "status")
    missing = [f for f in required_fields if f not in raw_event]
    if missing:
        raise EventValidationError(f"Missing field(s): {', '.join(missing)}")

    timestamp = parse_timestamp(raw_event["timestamp"])

    route = str(raw_event["route"]).strip()
    if not route:
        raise EventValidationError("Route must not be empty")

    bus = str(raw_event["bus"]).strip()
    if not bus:
        raise EventValidationError("Bus id must not be empty")

    passengers = raw_event["passengers"]
    if not isinstance(passengers, int) or isinstance(passengers, bool) or passengers < 0:
        raise EventValidationError(f"Passengers must be a non-negative integer, got {passengers!r}")

    speed = raw_event["speed_kmh"]
    if not isinstance(speed, (int, float)) or isinstance(speed, bool) or not (MIN_SPEED <= speed <= MAX_SPEED):
        raise EventValidationError(f"Speed_kmh must be between {MIN_SPEED} and {MAX_SPEED}, got {speed!r}")

    status = str(raw_event["status"]).strip().upper()
    if status not in VALID_STATUSES:
        raise EventValidationError(f"Status must be one of {VALID_STATUSES}, got {status!r}")

    return ShuttleEvent(
        timestamp=timestamp,
        route=route,
        bus=bus,
        passengers=passengers,
        speed_kmh=speed,
        status=status,
    )


def event_generator(raw_events: list[dict]) -> Iterator[dict]:
    for raw_event in raw_events:
        yield raw_event


def stream_events(raw_events: list[dict]) -> Iterator[dict]:
    for raw_event in raw_events:
        yield raw_event


@dataclass
class ProcessingResult:
    event_number: int
    passengers: Optional[int]
    speed_kmh: Optional[float]
    status: Optional[str]
    processed: bool
    reason: str


def process_stream(raw_events: list[dict]) -> list[ProcessingResult]:
    results = []
    for i, raw_event in enumerate(stream_events(raw_events), start=1):
        try:
            event = validate_event(raw_event)
            results.append(
                ProcessingResult(
                    event_number=i,
                    passengers=event.passengers,
                    speed_kmh=event.speed_kmh,
                    status=event.status,
                    processed=True,
                    reason="OK - passed all validation rules",
                )
            )
        except EventValidationError as exc:
            results.append(
                ProcessingResult(
                    event_number=i,
                    passengers=raw_event.get("passengers"),
                    speed_kmh=raw_event.get("speed_kmh"),
                    status=raw_event.get("status"),
                    processed=False,
                    reason=str(exc),
                )
            )
    return results


def get_valid_events(raw_events: list[dict]) -> list[ShuttleEvent]:
    valid = []
    for raw_event in raw_events:
        try:
            valid.append(validate_event(raw_event))
        except EventValidationError:
            continue
    return valid


def average_passengers(events: list[ShuttleEvent]) -> float:
    if not events:
        return 0.0
    return sum(e.passengers for e in events) / len(events)


def max_passengers(events: list[ShuttleEvent]) -> int:
    return max((e.passengers for e in events), default=0)


def count_stopped(events: list[ShuttleEvent]) -> int:
    return sum(1 for e in events if e.status == "STOPPED")


def busiest_event(events: list[ShuttleEvent]) -> Optional[ShuttleEvent]:
    if not events:
        return None
    return max(events, key=lambda e: e.passengers)


def run_analysis(raw_events: list[dict]) -> dict:
    events = get_valid_events(raw_events)
    busiest = busiest_event(events)
    return {
        "average_passengers": round(average_passengers(events), 2),
        "max_passengers": max_passengers(events),
        "stopped_events": count_stopped(events),
        "busiest_minute": busiest.timestamp.strftime("%H:%M") if busiest else None,
        "busiest_bus": busiest.bus if busiest else None,
    }


if __name__ == "__main__":
    print("=== Part B: Streaming simulation results ===")
    for r in process_stream(RAW_EVENTS):
        print(r)

    print("\n=== Part D: Analysis ===")
    for k, v in run_analysis(RAW_EVENTS).items():
        print(f"{k}: {v}")
