from fastapi import FastAPI
from pydantic import BaseModel

from shuttle_stream import validate_event, EventValidationError

app = FastAPI(title="AITU Campus Shuttle Stream API")


class EventIn(BaseModel):
    timestamp: str
    route: str
    bus: str
    passengers: int
    speed_kmh: float
    status: str


def occupancy_category(passengers: int) -> str:
    if passengers <= 10:
        return "LOW"
    if passengers <= 20:
        return "MEDIUM"
    if passengers <= 30:
        return "HIGH"
    return "OVER_CAPACITY"


@app.post("/events")
def post_event(event: EventIn):
    raw = event.model_dump()
    try:
        validated = validate_event(raw)
        return {
            "result": "accepted",
            "validation_errors": [],
            "occupancy_category": occupancy_category(validated.passengers),
            "event": validated.to_dict(),
        }
    except EventValidationError as exc:
        return {
            "result": "rejected",
            "validation_errors": [str(exc)],
            "occupancy_category": None,
            "event": raw,
        }
