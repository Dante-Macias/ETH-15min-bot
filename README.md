# ETH-15min-bot

Bot that bets on Polymarket's recurring 15-minute "ETH Up or Down" markets using a
MACD signal computed from real ETH-USD price data.

The build runs in four phases:

| Phase | Goal | Status |
|---|---|---|
| 1 | Real 15-minute ETH-USD history in a CSV | **this branch** |
| 2 | Backtesting analyzer with no look-ahead bias | not started |
| 3 | Parameter search, chronologically split, breakeven-aware | not started |
| 4 | The live `eth15min_bot` package | not started |

---

## Phase 1 — data fetcher

### Files

| File | Responsibility |
|---|---|
| `coinbase_exchange.py` | Client for Coinbase Exchange's public candle endpoint: retries, chunked backwards walk, row validation, merge/de-dup |
| `fetch_history.py` | CLI: fetch → sort/de-dup → CSV → sanity report |
| `tests/test_fetch_history.py` | Offline tests against a fake HTTP layer (no network needed) |

### Usage

```bash
pip install -r requirements.txt

python3 fetch_history.py --days 365 --out data/eth_usd_15m.csv --verbose
python3 fetch_history.py --start 2024-01-01 --out data/eth_usd_15m.csv
python3 fetch_history.py --all --out data/eth_usd_15m_full.csv   # until the API runs dry
```

Output columns, fixed so the file plugs straight into Phase 2:

```
open_time, close_time, open, high, low, close, volume
```

`open_time`/`close_time` are ISO 8601 UTC, rows are ascending and de-duplicated.
Exit status is `0` on success, `1` if a hard sanity check failed (unsorted,
off-grid timestamps, NaN/inconsistent OHLC), `2` if nothing could be fetched.
Gaps are reported but are not treated as a failure — the venue does have real
outages.

Run the tests with:

```bash
python3 tests/test_fetch_history.py
```

### Why this endpoint

Coinbase Advanced Trade's endpoint literally named "Get Public Product Candles"
is **not** keyless despite the name — it wants a Bearer JWT from a Coinbase
Developer Platform account. The one that genuinely needs no account is the
legacy Coinbase Exchange (ex-Coinbase Pro) API:

```
GET https://api.exchange.coinbase.com/products/ETH-USD/candles
    ?start=<iso8601>&end=<iso8601>&granularity=900
```

Valid granularities are only `60, 300, 900, 3600, 21600, 86400`.

### Pitfalls this code handles

- **Candle rows are `[time, low, high, open, close, volume]`** — index 4 is
  close, index 5 is volume. Mixing those up doesn't crash anything, it just
  silently corrupts every downstream MACD value. `test_column_indices_map_to_the_right_fields`
  pins this.
- **300 candles per response, max.** Longer histories are walked backwards in
  chunks of 299 candles (one under the cap, so inclusive bounds can't tip it
  over), with a configurable `--pace` sleep between requests.
- **Responses are typically newest-first, and chunks overlap.** Merging sorts
  by `row[0]` and de-duplicates on that same timestamp — never on the whole
  row: raw parsed rows are plain lists, which are unhashable, and sorting
  lists lexicographically only agrees with time order by luck.
- **429 / 5xx** are retried with exponential backoff (honouring `Retry-After`);
  other 4xx fail fast. A mid-walk failure still writes the pages already
  fetched rather than throwing them away.

### Network note

`api.exchange.coinbase.com` must be reachable from wherever this runs. In a
locked-down environment the request fails with a proxy `403` and the fetcher
reports it rather than silently writing a short file.
