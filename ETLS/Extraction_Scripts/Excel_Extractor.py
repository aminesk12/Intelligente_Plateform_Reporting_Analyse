
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from ETLS.core.base_extractor import BaseExtractor
from ETLS.core.config import load_yaml_config
from ETLS.core.exceptions import ExtractionError, ValidationError

SUPPORTED_EXTENSIONS = {".xlsx", ".xlsm", ".xls"}
# .xls requires the optional `xlrd` engine; .xlsx/.xlsm use openpyxl.


class ExcelExtractor(BaseExtractor):
    """Extract a sheet from an Excel workbook into a DataFrame.

    Parameters
    ----------
    file_path:
        Path to the workbook.
    sheet_name:
        Sheet to read — name (str), index (int), or ``None`` for the first.
    read_options:
        Extra keyword arguments forwarded to :func:`pandas.read_excel`
        (e.g. ``header``, ``skiprows``, ``usecols``, ``dtype``).
    """

    source_type = "excel"

    def __init__(
        self,
        file_path: str | Path,
        *,
        sheet_name: str | int = 0,
        read_options: dict[str, Any] | None = None,
        name: str | None = None,
    ) -> None:
        super().__init__(name=name or Path(file_path).name)
        self.file_path = Path(file_path)
        self.sheet_name = sheet_name
        self.read_options = read_options or {}

    @classmethod
    def from_config(cls, path: str | Path, *, section: str = "excel") -> "ExcelExtractor":
        """Build an extractor from a YAML config section."""
        cfg = load_yaml_config(path, section=section)
        return cls(
            file_path=cfg["file_path"],
            sheet_name=cfg.get("sheet_name", 0),
            read_options=cfg.get("read_options"),
            name=cfg.get("name"),
        )

    def validate(self) -> None:
        if not self.file_path.exists():
            raise ValidationError(f"File not found: {self.file_path}")
        if not self.file_path.is_file():
            raise ValidationError(f"Not a file: {self.file_path}")
        suffix = self.file_path.suffix.lower()
        if suffix not in SUPPORTED_EXTENSIONS:
            raise ValidationError(
                f"Unsupported Excel format '{suffix}'. "
                f"Expected one of {sorted(SUPPORTED_EXTENSIONS)}"
            )

    def _engine_for(self, suffix: str) -> str:
        return "xlrd" if suffix == ".xls" else "openpyxl"

    def _extract(self, sheet_name: str | int | None = None, **kwargs: Any) -> pd.DataFrame:
        sheet = self.sheet_name if sheet_name is None else sheet_name
        engine = self._engine_for(self.file_path.suffix.lower())
        self.logger.debug("Reading %s (sheet=%s, engine=%s)", self.file_path, sheet, engine)
        try:
            return pd.read_excel(
                self.file_path,
                sheet_name=sheet,
                engine=engine,
                **{**self.read_options, **kwargs},
            )
        except ImportError as exc:
            raise ExtractionError(
                f"Missing engine '{engine}' for {self.file_path.suffix}. "
                "Install it (e.g. `pip install xlrd` for .xls files)."
            ) from exc

    def _build_metadata(self, df: pd.DataFrame, **kwargs: Any) -> dict[str, Any]:
        return {
            "file_path": str(self.file_path),
            "sheet_name": kwargs.get("sheet_name", self.sheet_name),
            "file_size_bytes": self.file_path.stat().st_size,
        }
