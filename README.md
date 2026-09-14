# Assignment 1 — Python Fundamentals for Data Streaming
AITU Campus Shuttle and Mobility Data (synthetic dataset)

## Files

| File | Purpose |
|---|---|
| `shuttle_stream.py` | Part A (data structures, parsing, validation), Part B (streaming generator + simulation), Part D (analysis) |
| `generate_json_sample.py` | Part C — converts the dataset to JSON (`events_sample.json`) |
| `api.py` | Part C — FastAPI app with `POST /events` |
| `test_shuttle_stream.py` | Part E — automated tests (7 cases) |
| `events_sample.json` | Generated JSON sample of the validated dataset |

## How to run

```bash
pip install fastapi uvicorn pytest

# Part A/B/D: run the core module (prints streaming table + analysis)
python3 shuttle_stream.py

# Part C: generate the JSON sample
python3 generate_json_sample.py

# Part C: start the API
uvicorn api:app --reload
# then in another terminal:
curl -X POST http://127.0.0.1:8000/events \
     -H "Content-Type: application/json" \
     -d '{"timestamp":"08:04","route":"AITU-Campus-Residence","bus":"B01","passengers":24,"speed_kmh":0,"status":"STOPPED"}'

# Part E: run tests
python3 -m pytest test_shuttle_stream.py -v
```

## Part A — Validation rules

| Field | Python type | Validation rule | Example |
|---|---|---|---|
| Timestamp | `datetime.time` | must parse as `HH:MM` (24h) | `08:04` |
| Passengers | `int` | non-negative integer | `24` |
| Speed_kmh | `int`/`float` | `0 <= speed <= 120` | `31` |
| Status | `str` | `ON_ROUTE` or `STOPPED` | `ON_ROUTE` |

## Part B — Streaming simulation table

| Event | Passengers | Speed | Status | Processed? | Reason |
|---|---|---|---|---|---|
| 1 (08:00, B01) | 18 | 31 | ON_ROUTE | Yes | OK — passed all validation rules |
| 2 (08:01, B02) | 22 | 28 | ON_ROUTE | Yes | OK — passed all validation rules |
| 3 (08:02, B01) | 21 | 29 | ON_ROUTE | Yes | OK — passed all validation rules |
| 4 (08:03, B02) | 25 | 27 | ON_ROUTE | Yes | OK — passed all validation rules |
| 5 (08:04, B01) | 24 | 0 | STOPPED | Yes | OK — speed 0 is valid for a stopped bus |

(Events 6–10 all pass validation the same way — see full output of `python3 shuttle_stream.py`.)

## Part C — Occupancy categories

| Passenger count | Category |
|---|---|
| 0–10 | LOW |
| 11–20 | MEDIUM |
| 21–30 | HIGH |
| >30 | OVER_CAPACITY |

`POST /events` returns `result` (accepted/rejected), `validation_errors`, and `occupancy_category`.

## Part D — Analysis results (computed from the given dataset)

- Average passenger count: **24.4**
- Maximum passenger count: **30**
- Number of STOPPED events: **1**
- Busiest minute/bus: **08:09, B02** (30 passengers)

**Why generators instead of loading the whole stream into memory:**
A real shuttle feed is effectively unbounded — it keeps producing new events for as
long as the service runs. If every event were appended to a list before processing,
memory use would grow without limit and nothing could be processed until the whole
(infinite) stream had arrived. A generator (`yield`) produces one event at a time on
demand: the processing function consumes and discards each event before requesting
the next, so memory use stays constant (O(1)) regardless of stream length, and
results are available immediately as each event arrives — which matters for
real-time monitoring.

## Design notes

- `ShuttleEvent` is a `dataclass`, which keeps field access typed and readable while
  auto-generating `__init__`/`__repr__`.
- Validation raises a specific `EventValidationError` with a human-readable message,
  so both the streaming simulation and the API can report *why* an event was
  rejected instead of just a boolean.
- `stream_events`/`event_generator` are plain generators (`yield`), not lists, to
  satisfy the "do not load the whole stream into memory" requirement.

## Known limitation

The current validator assumes `timestamp` values are in `HH:MM` format with no date
component, since the source data has no date field. A production version would need
a full datetime (with date) to correctly order events across midnight.
