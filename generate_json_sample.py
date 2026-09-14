import json
from shuttle_stream import RAW_EVENTS, validate_event, EventValidationError


def events_to_json(raw_events: list[dict]) -> str:
    validated = []
    for raw_event in raw_events:
        try:
            validated.append(validate_event(raw_event).to_dict())
        except EventValidationError:
            continue
    return json.dumps(validated, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    output = events_to_json(RAW_EVENTS)
    with open("events_sample.json", "w", encoding="utf-8") as f:
        f.write(output)
    print(output)
