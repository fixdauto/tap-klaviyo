"""REST client handling, including KlaviyoStream base class."""

from __future__ import annotations

import logging
import random
import time
import typing as t
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl

from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.exceptions import RetriableAPIError
from singer_sdk.pagination import BaseHATEOASPaginator
from singer_sdk.streams import RESTStream

if t.TYPE_CHECKING:
    from urllib.parse import ParseResult

    import requests

logger = logging.getLogger(__name__)

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
UTC = timezone.utc
DEFAULT_START_DATE = datetime(2000, 1, 1, tzinfo=UTC).isoformat()


def _isodate_from_date_string(date_string: str) -> str:
    """Convert a date string to an ISO date string.

    Args:
        date_string: The date string to convert.

    Returns:
        An ISO date string.
    """
    return datetime.strptime(date_string, "%Y-%m-%d").replace(tzinfo=UTC).isoformat()


class KlaviyoPaginator(BaseHATEOASPaginator):
    """HATEOAS paginator for the Klaviyo API."""

    def get_next_url(self, response: requests.Response) -> str:
        data = response.json()
        if data is not None and data.get("links") is not None:
            return data.get("links").get("next")  # type: ignore[no-any-return]


class KlaviyoStream(RESTStream):
    """Klaviyo stream class."""

    url_base = "https://a.klaviyo.com/api"
    records_jsonpath = "$[data][*]"
    max_page_size: int | None = None

    # Shared across all stream instances in this tap process to avoid bursts
    _last_request_ts: float = 0.0

    # ---------------------------
    # Rate limit / throttle knobs
    # ---------------------------
    @property
    def min_request_interval_seconds(self) -> float:
        """Minimum spacing between HTTP requests (global across streams).

        Configure in Meltano:
          min_request_interval_seconds: 0.75  # ~1.3 req/sec
        """
        return float(self.config.get("min_request_interval_seconds", 0.5))

    @property
    def rate_limit_max_retries(self) -> int:
        """Max retry attempts when receiving 429 responses."""
        return int(self.config.get("rate_limit_max_retries", 20))

    @property
    def rate_limit_max_sleep_seconds(self) -> float:
        """Upper bound on a single sleep when rate-limited."""
        return float(self.config.get("rate_limit_max_sleep_seconds", 600))

    def request_decorator(self, func):
        """Throttle every request + handle 429s by respecting Retry-After.

        Important: overriding request_decorator replaces Singer SDK's default
        retry/backoff wrapper. This implementation preserves retry behavior for 429s.
        """

        def wrapped(prepared_request, context):
            attempt = 0

            while True:
                # ---- throttle every request (global) ----
                min_interval = self.min_request_interval_seconds
                if min_interval and min_interval > 0:
                    now = time.monotonic()
                    elapsed = now - KlaviyoStream._last_request_ts
                    if elapsed < min_interval:
                        # small jitter reduces synchronized retry bursts
                        sleep_s = (min_interval - elapsed) + random.uniform(0, 0.05)
                        self.logger.debug("Throttling request: sleeping %.3fs", sleep_s)
                        time.sleep(sleep_s)
                    KlaviyoStream._last_request_ts = time.monotonic()

                try:
                    return func(prepared_request, context)

                except RetriableAPIError as ex:
                    resp = getattr(ex, "response", None)
                    status = getattr(resp, "status_code", None)

                    # Only special-handle 429 here; otherwise bubble it up.
                    if status != 429:
                        raise

                    attempt += 1
                    if attempt > self.rate_limit_max_retries:
                        raise

                    retry_after = None
                    if resp is not None:
                        retry_after = resp.headers.get("Retry-After")

                    # Prefer Retry-After if present; otherwise exponential fallback
                    if retry_after:
                        try:
                            sleep_s = float(retry_after)
                        except ValueError:
                            sleep_s = min(2 ** (attempt - 1), 60.0)
                    else:
                        sleep_s = min(2 ** (attempt - 1), 60.0)

                    sleep_s = min(sleep_s, self.rate_limit_max_sleep_seconds)
                    sleep_s += random.uniform(0, 1.0)

                    self.logger.warning(
                        "Klaviyo rate limited (429). Sleeping %.2fs (Retry-After=%r), attempt %d/%d",
                        sleep_s,
                        retry_after,
                        attempt,
                        self.rate_limit_max_retries,
                    )
                    time.sleep(sleep_s)

        return wrapped

    # ---------------------------
    # Auth + headers + pagination
    # ---------------------------
    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="Authorization",
            value=f'Klaviyo-API-Key {self.config.get("auth_token", "")}',
            location="header",
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers: dict[str, str] = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        if "revision" in self.config:
            headers["revision"] = self.config.get("revision")
        return headers

    def get_new_paginator(self) -> BaseHATEOASPaginator:
        return KlaviyoPaginator()

    # ---------------------------
    # Query params
    # ---------------------------
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, t.Any]:
        params: dict[str, t.Any] = {}

        if next_page_token:
            params.update(parse_qsl(next_page_token.query))

        if self.replication_key:
            if self.get_starting_timestamp(context):
                filter_timestamp = self.get_starting_timestamp(context)
            elif self.config.get("start_date"):
                # FIX: self.config is dict-like; use .get()
                filter_timestamp = _isodate_from_date_string(self.config.get("start_date"))
            else:
                filter_timestamp = DEFAULT_START_DATE

            if self.is_sorted:
                params["sort"] = self.replication_key

            params["filter"] = f"greater-than({self.replication_key},{filter_timestamp})"

        if self.max_page_size:
            params["page[size]"] = self.max_page_size

        return params