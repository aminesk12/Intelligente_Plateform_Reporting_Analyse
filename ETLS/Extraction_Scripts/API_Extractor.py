"""REST / HTTP API extractor.

Features
--------
* Pluggable authentication: none, API key (header or query), Bearer token,
  or HTTP Basic.
* Automatic pagination: ``page`` number, ``offset``/``limit``, or
  ``cursor``/next-link styles.
* Retries with exponential backoff on transient network/5xx errors.
* JSON -> DataFrame flattening via a configurable ``records_path``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

try:
    import requests
    from requests import Response, Session
    from requests.auth import HTTPBasicAuth
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "APIExtractor requires the `requests` library. Install with `pip install requests`."
    ) from exc

from ETLS.core.base_extractor import BaseExtractor
from ETLS.core.config import load_yaml_config
from ETLS.core.exceptions import ExtractionError, SourceConnectionError, ValidationError
from ETLS.core.retry import with_retry

# Network errors worth retrying.
_RETRYABLE = (requests.ConnectionError, requests.Timeout)


class APIExtractor(BaseExtractor):
    """Extract JSON data from an HTTP API into a DataFrame.

    Parameters
    ----------
    base_url:
        Full URL of the endpoint to read.
    auth:
        Auth spec, e.g. ``{"type": "bearer", "token": "..."}``,
        ``{"type": "api_key", "key": "...", "name": "X-API-Key", "in": "header"}``,
        or ``{"type": "basic", "username": "...", "password": "..."}``.
    pagination:
        Pagination spec — see :meth:`_paginate`. ``None`` reads a single page.
    records_path:
        Dotted path to the list of records inside each JSON response
        (e.g. ``"data.items"``). ``None`` treats the whole body as the records.
    headers, params:
        Extra static headers / query parameters sent on every request.
    timeout:
        Per-request timeout in seconds.
    """

    source_type = "api"

    def __init__(
        self,
        base_url: str,
        *,
        method: str = "GET",
        auth: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        pagination: dict[str, Any] | None = None,
        records_path: str | None = None,
        timeout: float = 30.0,
        max_pages: int = 1000,
        name: str | None = None,
    ) -> None:
        super().__init__(name=name or base_url)
        self.base_url = base_url
        self.method = method.upper()
        self.auth_spec = auth or {"type": "none"}
        self.headers = headers or {}
        self.params = params or {}
        self.pagination = pagination
        self.records_path = records_path
        self.timeout = timeout
        self.max_pages = max_pages
        self._session: Session | None = None

    @classmethod
    def from_config(cls, path: str | Path, *, section: str = "api") -> "APIExtractor":
        cfg = load_yaml_config(path, section=section)
        return cls(
            base_url=cfg["base_url"],
            method=cfg.get("method", "GET"),
            auth=cfg.get("auth"),
            headers=cfg.get("headers"),
            params=cfg.get("params"),
            pagination=cfg.get("pagination"),
            records_path=cfg.get("records_path"),
            timeout=cfg.get("timeout", 30.0),
            max_pages=cfg.get("max_pages", 1000),
            name=cfg.get("name"),
        )

    # -- session / auth --------------------------------------------------------

    @property
    def session(self) -> Session:
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update(self.headers)
            self._apply_auth(self._session)
        return self._session

    def _apply_auth(self, session: Session) -> None:
        spec = self.auth_spec
        kind = spec.get("type", "none").lower()
        if kind in ("none", ""):
            return
        if kind == "bearer":
            session.headers["Authorization"] = f"Bearer {spec['token']}"
        elif kind == "api_key":
            location = spec.get("in", "header").lower()
            key_name = spec.get("name", "X-API-Key")
            if location == "header":
                session.headers[key_name] = spec["key"]
            elif location == "query":
                self.params[key_name] = spec["key"]
            else:
                raise ValidationError(f"api_key 'in' must be header|query, got {location!r}")
        elif kind == "basic":
            session.auth = HTTPBasicAuth(spec["username"], spec["password"])
        else:
            raise ValidationError(f"Unsupported auth type: {kind!r}")

    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None

    def __enter__(self) -> "APIExtractor":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- contract --------------------------------------------------------------

    def validate(self) -> None:
        if not self.base_url.lower().startswith(("http://", "https://")):
            raise ValidationError(f"base_url must be http(s): {self.base_url!r}")
        try:
            self._apply_auth(requests.Session())
        except KeyError as exc:
            raise ValidationError(f"Auth spec missing required field: {exc}") from exc

    @with_retry(attempts=4, base_delay=1.0, exceptions=_RETRYABLE)
    def _request(self, url: str, params: dict[str, Any]) -> Response:
        try:
            resp = self.session.request(
                self.method, url, params=params, timeout=self.timeout
            )
        except _RETRYABLE:
            raise
        except requests.RequestException as exc:
            raise SourceConnectionError(f"Request to {url} failed: {exc}") from exc
        # Retry on 5xx; raise immediately on 4xx.
        if resp.status_code >= 500:
            raise requests.ConnectionError(f"Server error {resp.status_code} from {url}")
        if resp.status_code >= 400:
            raise ExtractionError(
                f"HTTP {resp.status_code} from {url}: {resp.text[:300]}"
            )
        return resp

    def _extract(self, **kwargs: Any) -> pd.DataFrame:
        records: list[Any] = []
        pages = 0
        for body in self._paginate():
            page_records = self._extract_records(body)
            records.extend(page_records)
            pages += 1
            if pages >= self.max_pages:
                self.logger.warning("Reached max_pages=%d; stopping.", self.max_pages)
                break
        self._page_count = pages
        return pd.json_normalize(records) if records else pd.DataFrame()

    # -- pagination ------------------------------------------------------------

    def _paginate(self):
        """Yield successive response bodies according to the pagination spec."""
        spec = self.pagination
        if not spec:
            yield self._request(self.base_url, dict(self.params)).json()
            return

        style = spec.get("type", "page").lower()
        if style == "page":
            yield from self._paginate_page(spec)
        elif style == "offset":
            yield from self._paginate_offset(spec)
        elif style == "cursor":
            yield from self._paginate_cursor(spec)
        else:
            raise ValidationError(f"Unsupported pagination type: {style!r}")

    def _paginate_page(self, spec: dict[str, Any]):
        param = spec.get("page_param", "page")
        size_param = spec.get("size_param", "page_size")
        page = spec.get("start", 1)
        size = spec.get("page_size", 100)
        while True:
            params = {**self.params, param: page, size_param: size}
            body = self._request(self.base_url, params).json()
            records = self._extract_records(body)
            if not records:
                return
            yield body
            if len(records) < size:
                return
            page += 1

    def _paginate_offset(self, spec: dict[str, Any]):
        offset_param = spec.get("offset_param", "offset")
        limit_param = spec.get("limit_param", "limit")
        offset = spec.get("start", 0)
        limit = spec.get("limit", 100)
        while True:
            params = {**self.params, offset_param: offset, limit_param: limit}
            body = self._request(self.base_url, params).json()
            records = self._extract_records(body)
            if not records:
                return
            yield body
            if len(records) < limit:
                return
            offset += limit

    def _paginate_cursor(self, spec: dict[str, Any]):
        """Follow a next-page cursor/link found at ``next_path`` in each body."""
        next_path = spec.get("next_path", "next")
        cursor_param = spec.get("cursor_param")
        url = self.base_url
        params = dict(self.params)
        while url:
            body = self._request(url, params).json()
            yield body
            nxt = self._dig(body, next_path)
            if not nxt:
                return
            if cursor_param:
                # next_path holds an opaque cursor token to feed back as a param.
                params = {**self.params, cursor_param: nxt}
            else:
                # next_path holds a full URL to follow.
                url, params = str(nxt), {}

    # -- helpers ---------------------------------------------------------------

    def _extract_records(self, body: Any) -> list[Any]:
        data = self._dig(body, self.records_path) if self.records_path else body
        if data is None:
            return []
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        raise ExtractionError(
            f"Expected list/dict at records_path={self.records_path!r}, "
            f"got {type(data).__name__}"
        )

    @staticmethod
    def _dig(body: Any, dotted: str) -> Any:
        cur = body
        for key in dotted.split("."):
            if isinstance(cur, dict):
                cur = cur.get(key)
            else:
                return None
        return cur

    def _build_metadata(self, df: pd.DataFrame, **kwargs: Any) -> dict[str, Any]:
        return {
            "base_url": self.base_url,
            "method": self.method,
            "pages_fetched": getattr(self, "_page_count", 1),
        }
