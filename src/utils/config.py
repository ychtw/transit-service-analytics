import os
from pathlib import Path

import yaml


def get_config():
    config_path = Path("config/settings.yml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
