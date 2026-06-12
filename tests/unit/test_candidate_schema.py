from decimal import Decimal
from tradebot.events.candidate import CandidateSnapshot, CandidateSnapshotEvent

def test_candidate_snapshot_event_parses_scanner_payload():
    candidate = {
        "symbol": "SPCE",
        "update_time": "2026-06-09T20:09:19.569531+00:00",
        "scanner_tags": ["small_cap_gainers"],
        "price": "4.58",
        "volume": "54329445",
        "dollar_volume": "248828858.10",
        "previous_close": "4.12",
        "day_high": "4.89",
        "day_low": "4.18",
        "open": "4.23",
        "bid": "4.58",
        "ask": "4.61",
        "short_name": "Virgin Galactic Holdings, Inc.",
        "analyst_rating": None,
        "scanner_ts_ms": 1781035759950,
    }

    snapshot = CandidateSnapshot.model_validate(candidate)
    event = CandidateSnapshotEvent.from_candidate(snapshot)

    assert event.event_type == "candidate.snapshot.v1"
    assert event.schema_version == "1.0.0"
    assert event.candidate.symbol == "SPCE"
    assert event.candidate.price == Decimal("4.58")
    assert event.candidate.spread == Decimal("0.03")
    assert event.candidate.dollar_volume == Decimal("248828858.10")

    json_payload = event.model_dump(mode="json", exclude_none=True)
    assert json_payload["candidate"]["price"] == "4.58"
    assert json_payload["candidate"]["spread"] == "0.03"