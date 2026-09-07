#!/usr/bin/env python3
"""Phase 1 -- pull 15-minute ETH-USD history from Coinbase into a CSV.

    python3 fetch_history.py --days 365 --out data/eth_usd_15m.csv

The output columns are fixed so the file drops straight into the Phase 2
backtester without adaptation:

    open_time, close_time, open, high, low, close, volume

``open_time``/``close_time`` are ISO 8601 UTC; rows are ascending by time and
de-duplicated.  A sanity report (row count, range, gaps, NaNs) is printed at
the end and the exit status is non-zero if a hard check fails.
"""

from __future__ import annotations

import argparse
import csv
import logging
import math
import sys
from pathlib import Path

from coinbase_exchange import (
    IDX_CLOSE,
    IDX_HIGH,
    IDX_LOW,
    IDX_OPEN,
    IDX_TIME,
    IDX_VOLUME,
    VALID_GRANULARITIES,
    CoinbaseExchangeClient,
    CoinbaseExchangeError,
    days_ago,
    iso_utc,
    merge_candles,
    parse_utc_date,
    utc_now,
)

log = logging.getLogger("fetch_history")

CSV_COLUMNS = ["open_time", "close_time", "open", "high", "low", "close", "volume"]


def fmt_num(value: float) -> str:
    """Compact fixed-point rendering: no scientific notation, no trailing zeros."""
    text = f"{value:.8f}".rstrip("0").rstrip(".")
    return text or "0"


def rows_to_records(rows, granularity: int) -> list[dict[str, str]]:
    records = []
    for row in rows:
        open_epoch = int(row[IDX_TIME])
        records.append(
            {
                "open_time": iso_utc(open_epoch),
                "close_time": iso_utc(open_epoch + granularity),
                "open": fmt_num(row[IDX_OPEN]),
                "high": fmt_num(row[IDX_HIGH]),
                "low": fmt_num(row[IDX_LOW]),
                "close": fmt_num(row[IDX_CLOSE]),
                "volume": fmt_num(row[IDX_VOLUME]),
            }
        )
    return records


def write_csv(path: Path, records: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(records)
    tmp.replace(path)


def sanity_check(rows, granularity: int, max_gaps_shown: int = 10) -> tuple[bool, list[str]]:
    """Return (ok, report_lines).  ``ok`` is False only for hard failures."""
    lines: list[str] = []
    ok = True

    if not rows:
        return False, ["FAIL: no candles were returned at all"]

    times = [int(r[IDX_TIME]) for r in rows]
    first, last = times[0], times[-1]

    lines.append(f"rows:        {len(rows)}")
    lines.append(f"range:       {iso_utc(first)}  ..  {iso_utc(last)}")
    lines.append(f"span:        {(last - first) / 86400:.1f} days")

    if any(b <= a for a, b in zip(times, times[1:])):
        lines.append("FAIL: timestamps are not strictly ascending")
        ok = False

    off_grid = [t for t in times if t % granularity != 0]
    if off_grid:
        lines.append(
            f"FAIL: {len(off_grid)} timestamps are not multiples of {granularity}s "
            f"(first: {iso_utc(off_grid[0])})"
        )
        ok = False

    bad_values = 0
    for row in rows:
        prices = [row[IDX_OPEN], row[IDX_HIGH], row[IDX_LOW], row[IDX_CLOSE]]
        if any(math.isnan(v) or math.isinf(v) for v in prices + [row[IDX_VOLUME]]):
            bad_values += 1
        elif any(p <= 0 for p in prices):
            bad_values += 1
        elif not (row[IDX_LOW] <= min(row[IDX_OPEN], row[IDX_CLOSE])
                  and row[IDX_HIGH] >= max(row[IDX_OPEN], row[IDX_CLOSE])):
            bad_values += 1
    if bad_values:
        lines.append(f"FAIL: {bad_values} rows have NaN/non-positive/inconsistent OHLC values")
        ok = False
    else:
        lines.append("values:      no NaNs, all OHLC internally consistent")

    expected = (last - first) // granularity + 1
    missing = expected - len(rows)
    coverage = 100.0 * len(rows) / expected if expected else 0.0
    lines.append(f"coverage:    {len(rows)}/{expected} expected candles ({coverage:.2f}%)")

    gaps = [
        (a, b - a)
        for a, b in zip(times, times[1:])
        if b - a != granularity
    ]
    if gaps:
        lines.append(f"gaps:        {len(gaps)} discontinuities, {missing} candles missing")
        for start, delta in sorted(gaps, key=lambda g: -g[1])[:max_gaps_shown]:
            lines.append(
                f"             after {iso_utc(start)}: {delta // granularity - 1} missing "
                f"({delta / 60:.0f} min)"
            )
        if len(gaps) > max_gaps_shown:
            lines.append(f"             ... and {len(gaps) - max_gaps_shown} more")
    else:
        lines.append("gaps:        none -- fully contiguous")

    return ok, lines


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--product", default="ETH-USD", help="Coinbase product id (default: ETH-USD)")
    parser.add_argument(
        "--granularity", type=int, default=900, choices=VALID_GRANULARITIES,
        help="candle size in seconds (default: 900 = 15 minutes)",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--days", type=float, help="how many days back to fetch")
    group.add_argument("--start", help="earliest candle to fetch (YYYY-MM-DD or ISO 8601 UTC)")
    group.add_argument(
        "--all", action="store_true",
        help="walk back until the API stops returning candles",
    )
    parser.add_argument("--end", help="latest candle to fetch (default: now)")
    parser.add_argument("--out", default="data/eth_usd_15m.csv", help="output CSV path")
    parser.add_argument(
        "--pace", type=float, default=0.35,
        help="seconds to sleep between chunked requests (default: 0.35)",
    )
    parser.add_argument(
        "--max-chunks", type=int, default=2000,
        help="safety cap on the number of requests (default: 2000)",
    )
    parser.add_argument("--verbose", action="store_true", help="log every chunk")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    end = parse_utc_date(args.end) if args.end else utc_now()
    if args.start:
        floor = parse_utc_date(args.start)
    elif args.all:
        floor = None
    else:
        floor = days_ago(args.days if args.days else 365, end)

    print(
        f"Fetching {args.product} {args.granularity}s candles up to {iso_utc(end)}"
        + (f", back to {iso_utc(floor)}" if floor is not None else ", as far back as the API allows")
    )

    client = CoinbaseExchangeClient()
    pages = []
    try:
        for page in client.iter_history(
            product_id=args.product,
            granularity=args.granularity,
            end=end,
            floor=floor,
            max_chunks=args.max_chunks,
            pace_seconds=args.pace,
        ):
            pages.append(page)
            print(f"  ... {sum(len(p) for p in pages)} candles fetched", end="\r", flush=True)
    except CoinbaseExchangeError as exc:
        print()
        log.error("fetch aborted: %s", exc)
        if not pages:
            return 2
        log.warning("writing the %d pages fetched before the failure", len(pages))
    print()

    rows = merge_candles(pages)
    records = rows_to_records(rows, args.granularity)

    out_path = Path(args.out)
    write_csv(out_path, records)

    ok, report = sanity_check(rows, args.granularity)
    print(f"\nWrote {len(records)} rows to {out_path}")
    print("-" * 60)
    for line in report:
        print(line)
    print("-" * 60)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
