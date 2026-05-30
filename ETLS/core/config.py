"""Configuration loading for the ETL platform.

Reads YAML files and expands ``${ENV_VAR}`` / ``${ENV_VAR:default}`` references
from the process environment, so secrets (passwords, tokens) stay out of the
config files and live in environment variables or a ``.env`` file.
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any

import yaml

from ETLS.core.exceptions import ConfigError

logger = logging.getLogger(__name__)

# Matches ${VAR} and ${VAR:default}
_ENV_PATTERN = re.compile(r"\$\{([^}:]+)(?::([^}]*))?\}")

# Load a .env file once, if python-dotenv is available. Optional dependency.
try:  # pragma: no cover - convenience only
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # noqa: BLE001
    pass


def _expand(value: Any) -> Any:
    """Recursively expand ``${ENV}`` references in strings within a structure."""
    if isinstance(value, str):

        def _replace(match: re.Match[str]) -> str:
            var, default = match.group(1), match.group(2)
            resolved = os.environ.get(var)
            if resolved is None:
                if default is None:
                    raise ConfigError(
                        f"Environment variable '{var}' is referenced in config "
                        "but is not set, and no default was provided."
                    )
                return default
            return resolved

        return _ENV_PATTERN.sub(_replace, value)
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


def load_yaml_config(path: str | Path, *, section: str | None = None) -> dict[str, Any]:
    """Load a YAML config file and expand environment variables.

    Parameters
    ----------
    path:
        Path to the YAML file.
    section:
        Optional top-level key to return instead of the whole document.

    Raises
    ------
    ConfigError
        If the file is missing, not valid YAML, or the section is absent.
    """
    config_path = Path(path)
    if not config_path.exists():
        raise ConfigError(f"Config file not found: {config_path}")

    try:
        with config_path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {config_path}: {exc}") from exc

    if not isinstance(raw, dict):
        raise ConfigError(f"Config root in {config_path} must be a mapping.")

    expanded = _expand(raw)

    if section is not None:
        if section not in expanded:
            raise ConfigError(f"Section '{section}' not found in {config_path}")
        return expanded[section]

    logger.debug("Loaded config from %s", config_path)
    return expanded
