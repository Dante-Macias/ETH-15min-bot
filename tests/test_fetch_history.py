"""Offline tests for the Phase 1 fetcher.

These stand in for a live fetch: a fake HTTP layer replays realistic Coinbase
responses (newest-first, overlapping, occasionally malformed) so the parts
that are easy to get silently wrong -- column order, sorting, de-duplication,
chunk boundaries -- are checked without touching the network.
"""

from __future__ import annotations

import csv
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import coinbase_exchange as cx
import fetch_history as fh

GRAN = 900


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.headers = {}
        self.text = str(payload)

    def json(self):
        return self._payload


class FakeSession:
    """Serves synthetic candles newest-first, capped at 300 rows."""

    def __init__(self, first_epoch, last_epoch, seed=7):
        self.headers = {}
        self.first_epoch = first_epoch
        self.last_epoch = last_epoch
        self.requests = []
        rng = random.Random(seed)
        self.book = {}
        price = 2500.0
        for t in range(first_epoch, last_epoch + 1, GRAN):
            o = price
            c = price * (1 + rng.uniform(-0.004, 0.004))
            hi = max(o, c) * (1 + rng.uniform(0, 0.002))
            lo = min(o, c) * (1 - rng.uniform(0, 0.002))
            vol = rng.uniform(50, 500)
            # [time, low, high, open, close, volume]
            self.book[t] = [t, lo, hi, o, c, vol]
            price = c

    def get(self, url, params=None, timeout=None):
        self.requests.append(params)
        start = cx.parse_utc_date(params["start"])
        end = cx.parse_utc_date(params["end"])
        rows = [r for t, r in self.book.items() if start <= t <= end]
        rows.sort(key=lambda r: r[0], reverse=True)  # newest-first, like the real API
        return FakeResponse(rows[:300])


def make_client(session):
    return cx.CoinbaseExchangeClient(session=session, max_retries=2)


def test_column_indices_map_to_the_right_fields():
    row = [1700000000, 10.0, 40.0, 20.0, 30.0, 999.0]  # time, low, high, open, close, volume
    record = fh.rows_to_records([row], GRAN)[0]
    assert record["low"] == "10"
    assert record["high"] == "40"
    assert record["open"] == "20"
    assert record["close"] == "30", "index 4 must be close"
    assert record["volume"] == "999", "index 5 must be volume"
    assert record["open_time"] == "2023-11-14T22:13:20Z"
    assert record["close_time"] == "2023-11-14T22:28:20Z"


def test_merge_sorts_ascending_and_dedupes_by_timestamp():
    a = [[300, 1, 2, 1, 2, 9], [200, 1, 2, 1, 2, 9]]
    b = [[200, 1, 2, 1, 2, 9], [100, 1, 2, 1, 2, 9]]  # 200 overlaps page a
    merged = cx.merge_candles([a, b])
    assert [r[0] for r in merged] == [100, 200, 300]


def test_merge_handles_unhashable_rows_and_lexicographic_traps():
    # 1000 sorts before 999 lexicographically as strings; and plain lists are
    # unhashable, so a naive set() would raise. Neither must happen here.
    pages = [[[999, 1, 2, 1, 2, 3]], [[1000, 1, 2, 1, 2, 3]]]
    assert [r[0] for r in cx.merge_candles(pages)] == [999, 1000]


def test_malformed_rows_are_dropped_not_fatal():
    payload = [[1, 2, 3], [100, 1, 2, 1, 2, 3], ["x", 1, 2, 1, 2, 3], None]
    rows = cx._validate_rows(payload)
    assert [r[0] for r in rows] == [100]


def test_chunked_walk_covers_the_full_range_without_holes():
    last = 1_700_000_000 // GRAN * GRAN
    first = last - 1000 * GRAN  # ~4 chunks' worth
    session = FakeSession(first, last)
    client = make_client(session)
    pages = list(
        client.iter_history("ETH-USD", GRAN, end=last, floor=first, pace_seconds=0)
    )
    rows = cx.merge_candles(pages)
    assert len(rows) == 1001
    assert rows[0][0] == first and rows[-1][0] == last
    assert all(b[0] - a[0] == GRAN for a, b in zip(rows, rows[1:]))
    assert all(len(p) <= 300 for p in pages)


def test_walk_stops_after_consecutive_empty_pages():
    last = 1_700_000_000 // GRAN * GRAN
    first = last - 400 * GRAN
    session = FakeSession(first, last)
    client = make_client(session)
    pages = list(
        client.iter_history(
            "ETH-USD", GRAN, end=last, floor=None, stop_after_empty=2,
            max_chunks=50, pace_seconds=0,
        )
    )
    rows = cx.merge_candles(pages)
    assert len(rows) == 401
    assert len(session.requests) <= 5  # 2 pages of data + 2 empties, not 50


def test_retries_on_429_then_succeeds(monkeypatch=None):
    class FlakySession(FakeSession):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            self.calls = 0

        def get(self, url, params=None, timeout=None):
            self.calls += 1
            if self.calls == 1:
                return FakeResponse("rate limited", status_code=429)
            return super().get(url, params=params, timeout=timeout)

    last = 1_700_000_000 // GRAN * GRAN
    session = FlakySession(last - 10 * GRAN, last)
    client = cx.CoinbaseExchangeClient(session=session, max_retries=3)
    cx.time.sleep = lambda *_: None  # keep the test fast
    rows = client.get_candles("ETH-USD", last - 10 * GRAN, last, GRAN)
    assert len(rows) == 11 and session.calls == 2


def test_sanity_check_flags_a_gap_and_passes_clean_data(tmp_path=Path("/tmp")):
    last = 1_700_000_000 // GRAN * GRAN
    session = FakeSession(last - 50 * GRAN, last)
    rows = cx.merge_candles([list(session.book.values())])
    ok, report = fh.sanity_check(rows, GRAN)
    assert ok and any("fully contiguous" in line for line in report)

    holed = [r for r in rows if r[0] != rows[10][0]]
    ok2, report2 = fh.sanity_check(holed, GRAN)
    assert ok2, "a gap is a warning, not a hard failure"
    assert any("discontinuities" in line for line in report2)

    unsorted = list(reversed(rows))
    ok3, report3 = fh.sanity_check(unsorted, GRAN)
    assert not ok3 and any("ascending" in line for line in report3)


def test_end_to_end_csv_has_the_exact_phase2_columns():
    out = Path("/tmp/_phase1_test.csv")
    last = 1_700_000_000 // GRAN * GRAN
    first = last - 500 * GRAN
    session = FakeSession(first, last)
    client = make_client(session)
    pages = list(client.iter_history("ETH-USD", GRAN, end=last, floor=first, pace_seconds=0))
    records = fh.rows_to_records(cx.merge_candles(pages), GRAN)
    fh.write_csv(out, records)

    with out.open() as handle:
        reader = csv.reader(handle)
        header = next(reader)
        body = list(reader)
    assert header == ["open_time", "close_time", "open", "high", "low", "close", "volume"]
    assert len(body) == 501
    assert body[0][0] < body[-1][0], "CSV must be ascending by time"
    out.unlink()


def run_all():
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
            except AssertionError as exc:
                failures += 1
                print(f"FAIL {name}: {exc}")
            except Exception as exc:  # noqa: BLE001
                failures += 1
                print(f"ERROR {name}: {type(exc).__name__}: {exc}")
            else:
                print(f"ok   {name}")
    print("\n" + ("all tests passed" if not failures else f"{failures} test(s) failed"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(run_all())
