"""Client for Coinbase Exchange's public candle endpoint.

The endpoint used here is the legacy "Coinbase Exchange" (formerly Coinbase
Pro) API, which genuinely needs no account and no API key:

    GET https://api.exchange.coinbase.com/products/{product_id}/candles
        ?start=<iso8601>&end=<iso8601>&granularity=<seconds>

Note that Advanced Trade's "Get Public Product Candles" endpoint is *not* a
substitute: despite the name it requires a Bearer JWT from a Coinbase
Developer Platform account.

Two properties of the response drive most of the code below:

  * Each candle is ``[time, low, high, open, close, volume]`` -- index 4 is
    the close and index 5 is the volume.  Getting that order wrong does not
    raise, it silently corrupts everything downstream.
  * At most 300 candles come back per request, and the rows are typically
    newest-first.  Longer histories therefore have to be walked in chunks and
    sorted explicitly by the timestamp field.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator, Sequence

import requests

log = logging.getLogger(__name__)

API_ROOT = "https://api.exchange.coinbase.com"

# Only these granularities are accepted by the endpoint.
VALID_GRANULARITIES = (60, 300, 900, 3600, 21600, 86400)

# Hard server-side cap on rows per response.  We ask for one fewer so that
# inclusive start/end bounds cannot tip a request over the limit.
MAX_CANDLES_PER_REQUEST = 300
CANDLES_PER_CHUNK = MAX_CANDLES_PER_REQUEST - 1

# Column positions inside a raw candle row.
IDX_TIME, IDX_LOW, IDX_HIGH, IDX_OPEN, IDX_CLOSE, IDX_VOLUME = range(6)


class CoinbaseExchangeError(RuntimeError):
    """Raised when the endpoint cannot be read after exhausting retries."""


def iso_utc(epoch_seconds: int | float) -> str:
    """Render an epoch timestamp as an ISO 8601 UTC string."""
    dt = datetime.fromtimestamp(int(epoch_seconds), tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


class CoinbaseExchangeClient:
    def __init__(
        self,
        session: requests.Session | None = None,
        api_root: str = API_ROOT,
        timeout: float = 30.0,
        max_retries: int = 5,
        user_agent: str = "eth15min-bot/0.1 (+historical-candle-fetch)",
    ) -> None:
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": user_agent, "Accept": "application/json"})
        self.api_root = api_root.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries

    # -- single request -----------------------------------------------------

    def get_candles(
        self,
        product_id: str,
        start: int,
        end: int,
        granularity: int,
    ) -> list[list[float]]:
        """Fetch one page of candles for ``[start, end]`` (epoch seconds)."""
        if granularity not in VALID_GRANULARITIES:
            raise ValueError(
                f"granularity {granularity} is not one of {VALID_GRANULARITIES}"
            )

        url = f"{self.api_root}/products/{product_id}/candles"
        params = {
            "start": iso_utc(start),
            "end": iso_utc(end),
            "granularity": granularity,
        }

        delay = 1.0
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException as exc:  # network-level failure
                last_error = exc
                log.warning("request failed (attempt %d/%d): %s", attempt, self.max_retries, exc)
            else:
                if resp.status_code == 200:
                    return _validate_rows(resp.json())
                if resp.status_code == 429 or resp.status_code >= 500:
                    retry_after = resp.headers.get("Retry-After")
                    wait = float(retry_after) if retry_after and retry_after.isdigit() else delay
                    last_error = CoinbaseExchangeError(
                        f"HTTP {resp.status_code}: {resp.text[:200]}"
                    )
                    log.warning(
                        "HTTP %d (attempt %d/%d), sleeping %.1fs",
                        resp.status_code, attempt, self.max_retries, wait,
                    )
                    time.sleep(wait)
                    delay = min(delay * 2, 30.0)
                    continue
                # 4xx other than 429 will not get better by retrying.
                raise CoinbaseExchangeError(
                    f"HTTP {resp.status_code} for {url} {params}: {resp.text[:200]}"
                )

            time.sleep(delay)
            delay = min(delay * 2, 30.0)

        raise CoinbaseExchangeError(
            f"giving up on {url} {params} after {self.max_retries} attempts: {last_error}"
        )

    # -- chunked walk backwards --------------------------------------------

    def iter_history(
        self,
        product_id: str,
        granularity: int,
        end: int,
        floor: int | None = None,
        stop_after_empty: int = 3,
        max_chunks: int = 2000,
        pace_seconds: float = 0.35,
    ) -> Iterator[list[list[float]]]:
        """Yield pages of candles, walking backwards in time from ``end``.

        Stops at ``floor`` (epoch seconds) when given, after
        ``stop_after_empty`` consecutive empty pages, or after ``max_chunks``
        requests -- whichever comes first.  Pages are yielded exactly as the
        API returned them; ordering and de-duplication happen in the caller so
        that all pages are considered together.
        """
        span = CANDLES_PER_CHUNK * granularity
        cursor = int(end)
        empty_streak = 0

        for chunk_index in range(max_chunks):
            chunk_start = cursor - span
            if floor is not None:
                chunk_start = max(chunk_start, floor)
            if chunk_start >= cursor:
                return

            rows = self.get_candles(product_id, chunk_start, cursor, granularity)
            log.info(
                "chunk %d: %s .. %s -> %d candles",
                chunk_index + 1, iso_utc(chunk_start), iso_utc(cursor), len(rows),
            )

            if rows:
                empty_streak = 0
                yield rows
            else:
                empty_streak += 1
                if empty_streak >= stop_after_empty:
                    log.info("stopping: %d consecutive empty pages", empty_streak)
                    return

            if floor is not None and chunk_start <= floor:
                return

            cursor = chunk_start
            time.sleep(pace_seconds)


def _validate_rows(payload: object) -> list[list[float]]:
    """Coerce a parsed JSON body into a list of well-formed candle rows."""
    if not isinstance(payload, list):
        raise CoinbaseExchangeError(f"expected a JSON array, got {type(payload).__name__}")

    rows: list[list[float]] = []
    for raw in payload:
        if not isinstance(raw, (list, tuple)) or len(raw) < 6:
            log.warning("skipping malformed candle row: %r", raw)
            continue
        try:
            row = [float(raw[i]) for i in range(6)]
        except (TypeError, ValueError):
            log.warning("skipping non-numeric candle row: %r", raw)
            continue
        rows.append(row)
    return rows


def merge_candles(pages: Sequence[Sequence[Sequence[float]]]) -> list[list[float]]:
    """Flatten pages into one ascending, de-duplicated list of candles.

    De-duplication is keyed on the timestamp field alone.  Raw parsed rows are
    plain lists, which are unhashable, so a set of whole rows would raise; and
    sorting lists lexicographically only happens to agree with time order by
    luck.  Both operations use ``row[0]`` explicitly.
    """
    by_time: dict[int, list[float]] = {}
    for page in pages:
        for row in page:
            by_time[int(row[IDX_TIME])] = list(row)
    return [by_time[t] for t in sorted(by_time)]


def utc_now() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())


def parse_utc_date(text: str) -> int:
    """Parse ``YYYY-MM-DD`` or a full ISO 8601 timestamp into epoch seconds."""
    text = text.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def days_ago(days: float, from_epoch: int | None = None) -> int:
    base = from_epoch if from_epoch is not None else utc_now()
    return int(base - timedelta(days=days).total_seconds())
