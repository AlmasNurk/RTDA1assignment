import pytest

from shuttle_stream import (
    RAW_EVENTS,
    validate_event,
    EventValidationError,
    process_stream,
    run_analysis,
    event_generator,
)


def test_valid_event_is_accepted():
    raw = {"timestamp": "08:00", "route": "AITU-Campus-Residence", "bus": "B01",
           "passengers": 18, "speed_kmh": 31, "status": "ON_ROUTE"}
    event = validate_event(raw)
    assert event.bus == "B01"
    assert event.passengers == 18
    assert event.status == "ON_ROUTE"


def test_negative_passengers_is_rejected():
    raw = {"timestamp": "08:00", "route": "AITU-Campus-Residence", "bus": "B01",
           "passengers": -3, "speed_kmh": 31, "status": "ON_ROUTE"}
    with pytest.raises(EventValidationError):
        validate_event(raw)


def test_speed_out_of_range_is_rejected():
    raw = {"timestamp": "08:00", "route": "AITU-Campus-Residence", "bus": "B01",
           "passengers": 18, "speed_kmh": 150, "status": "ON_ROUTE"}
    with pytest.raises(EventValidationError):
        validate_event(raw)


def test_invalid_status_is_rejected():
    raw = {"timestamp": "08:00", "route": "AITU-Campus-Residence", "bus": "B01",
           "passengers": 18, "speed_kmh": 31, "status": "PARKED"}
    with pytest.raises(EventValidationError):
        validate_event(raw)


def test_process_stream_counts_all_ten_events_as_processed():
    results = process_stream(RAW_EVENTS)
    assert len(results) == 10
    assert all(r.processed for r in results)


def test_analysis_matches_expected_values():
    stats = run_analysis(RAW_EVENTS)
    assert stats["max_passengers"] == 30
    assert stats["stopped_events"] == 1
    assert stats["busiest_minute"] == "08:09"
    assert stats["busiest_bus"] == "B02"
    assert stats["average_passengers"] == pytest.approx(24.4, abs=0.01)


def test_event_generator_is_lazy_generator():
    gen = event_generator(RAW_EVENTS)
    assert hasattr(gen, "__next__")
    first = next(gen)
    assert first["bus"] == "B01"
