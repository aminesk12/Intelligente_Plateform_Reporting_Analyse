"""Local file loader: writes a DataFrame to disk in various formats.

Supported formats: CSV, Excel (.xlsx), JSON, Parquet (requires pyarrow).
The output path may contain ``{timestamp}`` and ``{name}`` placeholders that
are resolved at write time, making it easy to version output files without
manual naming.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from ETLS.core.base_loader import BaseLoader
from ETLS.core.config import load_yaml_config
from ETLS.core.exceptions import ValidationError


class FileLoader(BaseLoader):
    """Write a DataFrame to a local file.

    Parameters
    ----------
    path:
        Output file path.  May contain the placeholders ``{timestamp}``
        (replaced with ``YYYYMMDD_HHMMSS`` at write time) and ``{name}``
        (replaced with the loader's name).
    format:
        Output format — ``"csv"``, ``"excel"``, ``"json"``, or
        ``"parquet"``.  Inferred from the path extension when omitted.
    mode:
        Write mode for CSV and JSON: ``"w"`` to overwrite (default),
        ``"a"`` to append.  Ignored for Excel and Parquet (always overwrite).
    write_options:
        Extra keyword arguments forwarded verbatim to the underlying pandas
        write method.  For example ``{"sep": ";", "encoding": "utf-8"}``
        for CSV, or ``{"sheet_name": "Report"}`` for Excel.
    mkdir:
        Create parent directories automatically if they do not exist
        (default ``True``).

    Examples
    --------
    >>> loader = FileLoader("DATA/output/sales_{timestamp}.csv")
    >>> result = loader.load(transformation_result)

    >>> loader = FileLoader(
    ...     "DATA/reports/summary.xlsx",
    ...     write_options={"sheet_name": "Report", "freeze_panes": (1, 0)},
    ... )
    >>> result = loader.load(df)

    >>> # Append mode for incremental CSV loads
    >>> loader = FileLoader("DATA/logs/events.csv", mode="a")
    >>> result = loader.load(extraction_result)
    """

    loader_type = "file"

    _EXT_TO_FORMAT: dict[str, str] = {
        ".csv": "csv",
        ".xlsx": "excel",
        ".xls": "excel",
        ".json": "json",
        ".parquet": "parquet",
    }
    SUPPORTED_FORMATS: frozenset[str] = frozenset(_EXT_TO_FORMAT.values())

    def __init__(
        self,
        path: str | Path,
        *,
        format: str | None = None,
        mode: str = "w",
        write_options: dict[str, Any] | None = None,
        mkdir: bool = True,
        name: str | None = None,
    ) -> None:
        super().__init__(name=name)
        self.path_template = str(path)
        self.format = format
        self.mode = mode
        self.write_options = write_options or {}
        self.mkdir = mkdir

    @classmethod
    def from_config(
        cls, config_path: str | Path, *, section: str = "file"
    ) -> "FileLoader":
        """Instantiate from a YAML config file."""
        cfg = load_yaml_config(config_path, section=section)
        return cls(
            path=cfg["path"],
            format=cfg.get("format"),
            mode=cfg.get("mode", "w"),
            write_options=cfg.get("write_options"),
            mkdir=cfg.get("mkdir", True),
            name=cfg.get("name"),
        )

    # -- contract --------------------------------------------------------------

    def validate(self) -> None:
        # Resolve format using a dummy timestamp so no side-effects occur.
        dummy_path = Path(
            self.path_template.format(timestamp="check", name=self.name or "loader")
        )
        fmt = self._resolve_format(dummy_path)
        if fmt not in self.SUPPORTED_FORMATS:
            raise ValidationError(
                f"FileLoader: unsupported format '{fmt}'. "
                f"Supported: {sorted(self.SUPPORTED_FORMATS)}. "
                "Set 'format' explicitly if the extension is non-standard."
            )
        if self.mode not in {"w", "a"}:
            raise ValidationError(
                f"FileLoader: 'mode' must be 'w' or 'a'; got {self.mode!r}."
            )

    def _load(self, df: pd.DataFrame, **kwargs: Any) -> dict[str, Any]:
        path = self._resolve_path()
        fmt = self._resolve_format(path)
        opts = {**self.write_options, **kwargs}

        if self.mkdir:
            path.parent.mkdir(parents=True, exist_ok=True)

        if fmt == "csv":
            header = not (self.mode == "a" and path.exists())
            df.to_csv(path, mode=self.mode, header=header, index=False, **opts)

        elif fmt == "excel":
            df.to_excel(path, index=False, **opts)

        elif fmt == "json":
            orient = opts.pop("orient", "records")
            df.to_json(path, orient=orient, **opts)

        elif fmt == "parquet":
            try:
                df.to_parquet(path, index=False, **opts)
            except ImportError as exc:
                raise ImportError(
                    "FileLoader: Parquet format requires pyarrow. "
                    "Install with `pip install pyarrow`."
                ) from exc

        file_size = path.stat().st_size if path.exists() else None
        return {
            "path": str(path),
            "format": fmt,
            "mode": self.mode,
            "file_size_bytes": file_size,
        }

    def _destination_label(self) -> str:
        return self.path_template

    # -- helpers ---------------------------------------------------------------

    def _resolve_path(self) -> Path:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        resolved = self.path_template.format(timestamp=ts, name=self.name or "loader")
        return Path(resolved)

    def _resolve_format(self, path: Path) -> str:
        if self.format:
            return self.format.lower()
        ext = path.suffix.lower()
        return self._EXT_TO_FORMAT.get(ext, ext.lstrip("."))
