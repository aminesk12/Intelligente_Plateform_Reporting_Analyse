"""Download raw source files (Excel, SQLite, JSON) from a remote URL into DATA/.

Run from the project root:
    python Data_Downloader.py                      # run every job in the config
    python Data_Downloader.py <url> [dest_path]     # ad-hoc single download

Config: ETLS/config/download_config.yaml (section "downloads"), a list of jobs:
    - name: coffee_sales_raw
      url: "https://example.com/datasets/coffee_sales.xlsx"
      dest: "DATA/Excel/coffee_sales.xlsx"   # optional — inferred from the URL's
                                              # extension + a default folder if omitted
      overwrite: false                        # optional, default false (skip if already downloaded)
      auth:                                    # optional, same spec as APIExtractor
        type: bearer
        token: "${DOWNLOAD_TOKEN}"

Supported extensions -> default destination folder:
    .xlsx / .xls / .xlsm -> DATA/Excel
    .db / .sqlite / .sqlite3 -> DATA/database
    .json -> DATA/API
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

try:
    import requests
    from requests.auth import HTTPBasicAuth
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "Data_Downloader requires the `requests` library. Install with `pip install requests`."
    ) from exc

from ETLS.core.config import load_yaml_config
from ETLS.core.exceptions import SourceConnectionError, ValidationError
from ETLS.core.logging_config import setup_logging
from ETLS.core.retry import with_retry

logger = logging.getLogger("etl.download")

DOWNLOAD_CONFIG = "ETLS/config/download_config.yaml"

_DEFAULT_DEST_DIR = {
    ".xlsx": "DATA/Excel",
    ".xls": "DATA/Excel",
    ".xlsm": "DATA/Excel",
    ".db": "DATA/database",
    ".sqlite": "DATA/database",
    ".sqlite3": "DATA/database",
    ".json": "DATA/API",
}

SUPPORTED_EXTENSIONS = set(_DEFAULT_DEST_DIR)

# Network errors worth retrying (mirrors APIExtractor).
_RETRYABLE = (requests.ConnectionError, requests.Timeout)


class DataDownloader:
    """Stream a file from an HTTP(S) URL to a local path under DATA/."""

    def __init__(self, *, timeout: float = 30.0, chunk_size: int = 256 * 1024) -> None:
        self.timeout = timeout
        self.chunk_size = chunk_size

    @staticmethod
    def _auth_for(spec: dict[str, Any] | None) -> tuple[Any, dict[str, str]]:
        """Translate an APIExtractor-style auth spec into (requests auth, extra headers)."""
        if not spec:
            return None, {}
        kind = spec.get("type", "none").lower()
        if kind in ("none", ""):
            return None, {}
        if kind == "bearer":
            return None, {"Authorization": f"Bearer {spec['token']}"}
        if kind == "basic":
            return HTTPBasicAuth(spec["username"], spec["password"]), {}
        if kind == "api_key":
            location = spec.get("in", "header").lower()
            if location != "header":
                raise ValidationError("Download auth 'api_key' only supports 'in: header'")
            return None, {spec.get("name", "X-API-Key"): spec["key"]}
        raise ValidationError(f"Unsupported auth type: {kind!r}")

    @staticmethod
    def _default_dest(url: str) -> Path:
        filename = Path(url.split("?", 1)[0]).name
        suffix = Path(filename).suffix.lower()
        if suffix not in SUPPORTED_EXTENSIONS:
            raise ValidationError(
                f"Unsupported file type '{suffix}' for {url!r}. "
                f"Expected one of {sorted(SUPPORTED_EXTENSIONS)}"
            )
        return Path(_DEFAULT_DEST_DIR[suffix]) / filename

    @with_retry(attempts=4, base_delay=1.0, exceptions=_RETRYABLE)
    def _get(self, url: str, headers: dict[str, str], auth: Any) -> requests.Response:
        try:
            resp = requests.get(url, headers=headers, auth=auth, timeout=self.timeout, stream=True)
        except _RETRYABLE:
            raise
        except requests.RequestException as exc:
            raise SourceConnectionError(f"Request to {url} failed: {exc}") from exc
        if resp.status_code >= 500:
            raise requests.ConnectionError(f"Server error {resp.status_code} from {url}")
        if resp.status_code >= 400:
            raise SourceConnectionError(f"HTTP {resp.status_code} from {url}: {resp.text[:300]}")
        return resp

    def download(
        self,
        url: str,
        dest: str | Path | None = None,
        *,
        overwrite: bool = False,
        auth: dict[str, Any] | None = None,
        name: str | None = None,
    ) -> Path:
        """Download ``url`` to ``dest`` (or an extension-inferred default under DATA/)."""
        label = name or url
        dest_path = Path(dest) if dest else self._default_dest(url)
        if dest_path.suffix.lower() not in SUPPORTED_EXTENSIONS:
            raise ValidationError(
                f"Unsupported file type '{dest_path.suffix}' for destination {dest_path}. "
                f"Expected one of {sorted(SUPPORTED_EXTENSIONS)}"
            )

        if dest_path.exists() and not overwrite:
            logger.info("Skipping %s: %s already exists (overwrite=False)", label, dest_path)
            return dest_path

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        request_auth, extra_headers = self._auth_for(auth)

        logger.info("Downloading %s -> %s", label, dest_path)
        resp = self._get(url, extra_headers, request_auth)

        # Write to a temp file and rename atomically, so a failed/partial
        # download never leaves a corrupt file at the final path.
        tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")
        try:
            with open(tmp_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=self.chunk_size):
                    if chunk:
                        fh.write(chunk)
        finally:
            resp.close()
        tmp_path.replace(dest_path)

        logger.info("Saved %s (%d bytes) -> %s", label, dest_path.stat().st_size, dest_path)
        return dest_path

    def download_all(self, jobs: list[dict[str, Any]]) -> list[Path]:
        return [
            self.download(
                job["url"],
                job.get("dest"),
                overwrite=job.get("overwrite", False),
                auth=job.get("auth"),
                name=job.get("name"),
            )
            for job in jobs
        ]

    @classmethod
    def from_config(cls, path: str | Path, *, section: str = "downloads") -> list[dict[str, Any]]:
        """Load the list of download jobs from a YAML config section."""
        jobs = load_yaml_config(path, section=section)
        if not isinstance(jobs, list):
            raise ValidationError(f"Config section '{section}' must be a list of jobs.")
        return jobs


if __name__ == "__main__":
    setup_logging(level="INFO")
    downloader = DataDownloader()

    if len(sys.argv) > 1:
        cli_url = sys.argv[1]
        cli_dest = sys.argv[2] if len(sys.argv) > 2 else None
        saved = downloader.download(cli_url, cli_dest, overwrite=True)
        print(f"Downloaded -> {saved}")
    else:
        download_jobs = DataDownloader.from_config(DOWNLOAD_CONFIG)
        if not download_jobs:
            print(f"No download jobs configured in {DOWNLOAD_CONFIG} (section 'downloads').")
        else:
            for saved in downloader.download_all(download_jobs):
                print(f"-> {saved}")
