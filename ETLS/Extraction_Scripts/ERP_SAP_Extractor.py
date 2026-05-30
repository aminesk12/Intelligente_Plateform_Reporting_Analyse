"""SAP ERP extractor.

SAP can be read several ways; the most portable and least install-heavy is
**OData** (SAP Gateway / S/4HANA exposes business data as OData V2/V4 REST
services). That is what :class:`SAPODataExtractor` implements. It handles:

* SAP client selection (``sap-client``) and OData ``$format=json``.
* Server-side paging via ``$top``/``$skip`` and following ``__next`` /
  ``@odata.nextLink`` links.
* OData query options (``$filter``, ``$select``, ``$expand``, ``$orderby``).
* CSRF token fetch (needed by some gateways even for reads).
* Basic / Bearer authentication and retries.

Two alternative paths are intentionally left as optional integrations (they
require extra system-level SDKs, so they are not imported here):

* **RFC / BAPI** via ``pyrfc`` — requires the SAP NW RFC SDK installed on the
  host. Best for classic ABAP function modules.
* **SAP HANA** direct SQL via ``hdbcli`` / ``sqlalchemy-hana`` — in that case
  use :class:`~ETLS.Extraction_Scripts.DataBase_Extractor.DatabaseExtractor`
  with a ``hana+hdbcli://`` connection URL.
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
        "SAPODataExtractor requires `requests`. Install with `pip install requests`."
    ) from exc

from ETLS.core.base_extractor import BaseExtractor
from ETLS.core.config import load_yaml_config
from ETLS.core.exceptions import ExtractionError, SourceConnectionError, ValidationError
from ETLS.core.retry import with_retry

_RETRYABLE = (requests.ConnectionError, requests.Timeout)


class SAPODataExtractor(BaseExtractor):
    """Extract an OData entity set from an SAP Gateway / S/4HANA service.

    Parameters
    ----------
    base_url:
        Service document URL, e.g.
        ``https://host:443/sap/opu/odata/sap/ZSALES_SRV``.
    entity_set:
        Entity set to read, e.g. ``"SalesOrderSet"``.
    sap_client:
        SAP client (mandant) number, sent as the ``sap-client`` query param.
    odata_version:
        ``2`` or ``4``; controls record path and next-link parsing.
    query_options:
        OData system query options without the leading ``$``, e.g.
        ``{"filter": "Year eq 2025", "select": "Id,Amount", "orderby": "Id"}``.
    auth:
        ``{"type": "basic", "username": ..., "password": ...}`` or
        ``{"type": "bearer", "token": ...}``.
    page_size:
        ``$top`` page size used for server-side paging.
    fetch_csrf:
        If true, fetch an ``X-CSRF-Token`` before reading (some gateways
        require it even for GET).
    """

    source_type = "sap_odata"

    def __init__(
        self,
        base_url: str,
        entity_set: str,
        *,
        sap_client: str | None = None,
        odata_version: int = 2,
        query_options: dict[str, Any] | None = None,
        auth: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        page_size: int = 1000,
        max_pages: int = 1000,
        fetch_csrf: bool = False,
        verify_ssl: bool = True,
        timeout: float = 60.0,
        name: str | None = None,
    ) -> None:
        super().__init__(name=name or f"{entity_set}@{base_url}")
        self.base_url = base_url.rstrip("/")
        self.entity_set = entity_set
        self.sap_client = sap_client
        self.odata_version = odata_version
        self.query_options = query_options or {}
        self.auth_spec = auth or {"type": "none"}
        self.headers = headers or {}
        self.page_size = page_size
        self.max_pages = max_pages
        self.fetch_csrf = fetch_csrf
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self._session: Session | None = None

    @classmethod
    def from_config(cls, path: str | Path, *, section: str = "sap") -> "SAPODataExtractor":
        cfg = load_yaml_config(path, section=section)
        return cls(
            base_url=cfg["base_url"],
            entity_set=cfg["entity_set"],
            sap_client=cfg.get("sap_client"),
            odata_version=cfg.get("odata_version", 2),
            query_options=cfg.get("query_options"),
            auth=cfg.get("auth"),
            headers=cfg.get("headers"),
            page_size=cfg.get("page_size", 1000),
            max_pages=cfg.get("max_pages", 1000),
            fetch_csrf=cfg.get("fetch_csrf", False),
            verify_ssl=cfg.get("verify_ssl", True),
            timeout=cfg.get("timeout", 60.0),
            name=cfg.get("name"),
        )

    # -- session / auth --------------------------------------------------------

    @property
    def session(self) -> Session:
        if self._session is None:
            s = requests.Session()
            s.verify = self.verify_ssl
            s.headers.update({"Accept": "application/json", **self.headers})
            spec = self.auth_spec
            kind = spec.get("type", "none").lower()
            if kind == "basic":
                s.auth = HTTPBasicAuth(spec["username"], spec["password"])
            elif kind == "bearer":
                s.headers["Authorization"] = f"Bearer {spec['token']}"
            elif kind not in ("none", ""):
                raise ValidationError(f"Unsupported SAP auth type: {kind!r}")
            self._session = s
        return self._session

    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None

    def __enter__(self) -> "SAPODataExtractor":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- contract --------------------------------------------------------------

    def validate(self) -> None:
        if not self.base_url.lower().startswith(("http://", "https://")):
            raise ValidationError(f"base_url must be http(s): {self.base_url!r}")
        if not self.entity_set:
            raise ValidationError("entity_set is required.")
        if self.odata_version not in (2, 4):
            raise ValidationError("odata_version must be 2 or 4.")

    def _base_params(self) -> dict[str, Any]:
        params: dict[str, Any] = {"$format": "json"}
        if self.sap_client:
            params["sap-client"] = self.sap_client
        for key, value in self.query_options.items():
            params[f"${key.lstrip('$')}"] = value
        return params

    def _fetch_csrf_token(self) -> None:
        url = f"{self.base_url}/{self.entity_set}"
        resp = self.session.get(
            url,
            params={**self._base_params(), "$top": 0},
            headers={"X-CSRF-Token": "Fetch"},
            timeout=self.timeout,
        )
        token = resp.headers.get("X-CSRF-Token")
        if token:
            self.session.headers["X-CSRF-Token"] = token
            self.logger.debug("Fetched SAP CSRF token.")

    @with_retry(attempts=4, base_delay=2.0, exceptions=_RETRYABLE)
    def _get(self, url: str, params: dict[str, Any] | None) -> Response:
        try:
            resp = self.session.get(url, params=params, timeout=self.timeout)
        except _RETRYABLE:
            raise
        except requests.RequestException as exc:
            raise SourceConnectionError(f"SAP request to {url} failed: {exc}") from exc
        if resp.status_code >= 500:
            raise requests.ConnectionError(f"SAP server error {resp.status_code}")
        if resp.status_code == 401:
            raise SourceConnectionError("SAP authentication failed (401).")
        if resp.status_code >= 400:
            raise ExtractionError(
                f"SAP HTTP {resp.status_code} from {url}: {resp.text[:300]}"
            )
        return resp

    def _extract(self, **kwargs: Any) -> pd.DataFrame:
        if self.fetch_csrf:
            self._fetch_csrf_token()

        records: list[Any] = []
        url: str | None = f"{self.base_url}/{self.entity_set}"
        params: dict[str, Any] | None = {**self._base_params(), "$top": self.page_size}
        skip = 0
        pages = 0

        while url and pages < self.max_pages:
            if params is not None and self.odata_version == 2:
                params["$skip"] = skip
            body = self._get(url, params).json()
            batch = self._records(body)
            records.extend(batch)
            pages += 1

            next_url = self._next_link(body)
            if next_url:
                # Follow the server-provided link verbatim (already paged).
                url, params = next_url, None
            elif len(batch) == self.page_size:
                # No explicit link but a full page — page manually via $skip.
                skip += self.page_size
            else:
                url = None

        self._page_count = pages
        return pd.json_normalize(records) if records else pd.DataFrame()

    # -- OData response parsing ------------------------------------------------

    def _records(self, body: Any) -> list[Any]:
        if self.odata_version == 2:
            d = body.get("d", body) if isinstance(body, dict) else body
            results = d.get("results", d) if isinstance(d, dict) else d
        else:  # V4
            results = body.get("value", []) if isinstance(body, dict) else body
        if isinstance(results, list):
            return results
        if isinstance(results, dict):
            return [results]
        return []

    def _next_link(self, body: Any) -> str | None:
        if not isinstance(body, dict):
            return None
        if self.odata_version == 2:
            d = body.get("d", {})
            return d.get("__next") if isinstance(d, dict) else None
        return body.get("@odata.nextLink")

    def _build_metadata(self, df: pd.DataFrame, **kwargs: Any) -> dict[str, Any]:
        return {
            "base_url": self.base_url,
            "entity_set": self.entity_set,
            "sap_client": self.sap_client,
            "odata_version": self.odata_version,
            "pages_fetched": getattr(self, "_page_count", 1),
        }
