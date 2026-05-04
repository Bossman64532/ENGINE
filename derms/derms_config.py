import os
from pathlib import Path


def _read_simple_yaml_value(path: Path, key: str) -> str | None:
    if not path.exists():
        return None

    prefix = f"{key}:"
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith(prefix):
            return stripped[len(prefix):].strip().strip("\"'")

    return None


def get_base_url() -> str:
    env_url = os.environ.get("DERMS_BASE_URL")
    if env_url:
        return env_url.rstrip("/")

    config_path = Path(__file__).resolve().parent.parent / "Server" / "gridappsd-2030_5" / "config.yml"
    host = _read_simple_yaml_value(config_path, "server") or "localhost"
    port = _read_simple_yaml_value(config_path, "admin_http_port") or "5002"
    return f"http://{host}:{port}".rstrip("/")
