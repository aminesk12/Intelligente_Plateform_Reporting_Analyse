"""REST API loader: POST or PUT a DataFrame to an HTTP endpoint.

The DataFrame is serialised to JSON and sent as one request or in batches.
Supports the same auth schemes as :class:`~ETLS.Extraction_Scripts.API_Extractor`
(bearer token, API key header, HTTP Basic).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import requests
    from requests import Session
    from requests.auth import HTTPBasicAuth
    from requests.exceptions import HTTPError, RequestException
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "APILoader requires the 'requests' library.  "
        "Install with `pip install requests`."
    ) from exc

from ETLS.core.base_loader import BaseLoader
from ETLS.core.config import load_yaml_config
from ETLS.core.exceptions import DestinationConnectionError, LoadError, ValidationError
from ETLS.core.retry import with_retry


class APILoader(BaseLoader):
    """Send a DataFrame to a REST API endpoint as JSON.

    Parameters
    ----------
    url:
        Full endpoint URL, e.g. ``"https://api.example.com/v1/data"``.
    method:
        HTTP method — ``"POST"`` (default), ``"PUT"``, or ``"PATCH"``.
    auth:
        Authentication config dict.  Supported schemes:

        * ``{"type": "bearer", "token": "..."}``
        * ``{"type": "api_key", "key": "...", "header": "X-Api-Key"}``
        * ``{"type": "basic", "username": "...", "password": "..."}``
    batch_size:
        Number of rows per request.  ``None`` (default) sends all rows in a
        single request.  Use this when the API has a payload size limit.
    orient:
        JSON serialisation mode for ``pandas.DataFrame.to_json``.  Defaults
        to ``"records"`` (list of dicts, one per row).  Other useful values:
        ``"split"``, ``"values"``.
    extra_headers:
        Additional HTTP headers merged into every request.
    timeout:
        Per-request timeout in seconds (default 30).

    Examples
    --------
    >>> loader = APILoader(
    ...     "https://api.example.com/ingest",
    ...     auth={"type": "bearer", "token": "my-secret-token"},
    ...     batch_size=500,
    ... )
    >>> result = loader.load(transformation_result)

    >>> loader = APILoader.from_config("ETLS/config/api_load.yaml")
    >>> result = loader.load(df)

    >>> with APILoader("https://api.example.com/events", method="PUT") as loader:
    ...     result = loader.load(extraction_result)
    """

    loader_type = "api"

    _VALID_METHODS = {"POST", "PUT", "PATCH"}

    def __init__(
        self,
        url: str,
        *,
        method: str = "POST",
        auth: dict[str, Any] | None = None,
        batch_size: int | None = None,
        orient: str = "records",
        extra_headers: dict[str, str] | None = None,
        timeout: int = 30,
        name: str | None = None,
    ) -> None:
        super().__init__(name=name or url)
        self.url = url
        self.method = method.upper()
        self.auth_config = auth or {}
        self.batch_size = batch_size
        self.orient = orient
        self.extra_headers = extra_headers or {}
        self.timeout = timeout
        self._session: Session | None = None

    @classmethod
    def from_config(
        cls, path: str | Path, *, section: str = "api"
    ) -> "APILoader":
        """Instantiate from a YAML config file."""
        cfg = load_yaml_config(path, section=section)
        return cls(
            url=cfg["url"],
            method=cfg.get("method", "POST"),
            auth=cfg.get("auth"),
            batch_size=cfg.get("batch_size"),
            orient=cfg.get("orient", "records"),
            extra_headers=cfg.get("extra_headers"),
            timeout=cfg.get("timeout", 30),
            name=cfg.get("name"),
        )

    # -- session lifecycle -----------------------------------------------------

    @property
    def session(self) -> Session:
        if self._session is None:
            self._session = self._build_session()
        return self._session

    def _build_session(self) -> Session:
        s = requests.Session()
        s.headers.update({"Content-Type": "application/json"})
        s.headers.update(self.extra_headers)

        auth_type = self.auth_config.get("type", "").lower()
        if auth_type == "bearer":
            s.headers["Authorization"] = f"Bearer {self.auth_config['token']}"
        elif auth_type == "api_key":
            s.headers[self.auth_config["header"]] = self.auth_config["key"]
        elif auth_type == "basic":
            s.auth = HTTPBasicAuth(
                self.auth_config["username"], self.auth_config["password"]
            )
        return s

    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None

    def __enter__(self) -> "APILoader":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- contract --------------------------------------------------------------

    @with_retry(attempts=3, base_delay=1.0, exceptions=(RequestException,))
    def validate(self) -> None:
        """Validate config and confirm the endpoint is reachable."""
        if self.method not in self._VALID_METHODS:
            raise ValidationError(
                f"APILoader: 'method' must be one of {sorted(self._VALID_METHODS)}; "
                f"got {self.method!r}."
            )
        if not self.url.startswith(("http://", "https://")):
            raise ValidationError(
                f"APILoader: 'url' must start with http:// or https://; "
                f"got {self.url!r}."
            )
        # A HEAD request confirms reachability without sending data.
        # Any HTTP response (even 4xx/5xx) means the server is up.
        try:
            self.session.head(self.url, timeout=self.timeout)
        except RequestException as exc:
            raise DestinationConnectionError(
                f"APILoader: cannot reach '{self.url}': {exc}"
            ) from exc

    def _load(self, df: pd.DataFrame, **kwargs: Any) -> dict[str, Any]:
        if df.empty:
            self.logger.warning("Load skipped: input DataFrame is empty.")
            return {"skipped": True, "reason": "empty_dataframe"}

        batches = self._make_batches(df)
        status_codes: list[int] = []

        for i, batch in enumerate(batches, start=1):
            payload = json.loads(batch.to_json(orient=self.orient))
            self.logger.debug(
                "Sending batch %d/%d (%d rows) to %s",
                i, len(batches), len(batch), self.url,
            )
            try:
                resp = self.session.request(
                    self.method,
                    self.url,
                    json=payload,
                    timeout=self.timeout,
                )
                resp.raise_for_status()
                status_codes.append(resp.status_code)
            except HTTPError as exc:
                raise LoadError(
                    f"APILoader: server returned {exc.response.status_code} "
                    f"for batch {i}/{len(batches)}: "
                    f"{exc.response.text[:200]}"
                ) from exc

        return {
            "url": self.url,
            "method": self.method,
            "batches_sent": len(batches),
            "status_codes": status_codes,
        }

    def _destination_label(self) -> str:
        return self.url

    # -- helpers ---------------------------------------------------------------

    def _make_batches(self, df: pd.DataFrame) -> list[pd.DataFrame]:
        if not self.batch_size:
            return [df]
        return [
            df.iloc[i : i + self.batch_size]
            for i in range(0, len(df), self.batch_size)
        ]
