import os
import yaml
from typing import Any, Dict

def load_yaml_with_env(path: str) -> Dict[str, Any]:
    """Load a YAML file and expand ${VAR} / ${VAR:-default} using environment variables."""
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()

    # Use shell-style env expansion on the raw text
    expanded = os.path.expandvars(raw)
    return yaml.safe_load(expanded)