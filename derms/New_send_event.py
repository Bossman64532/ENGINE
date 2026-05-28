# New_send_event.py

import argparse
import dataclasses
import inspect
import time
import types
from pathlib import Path
from typing import Union, get_args, get_origin, get_type_hints

import requests
import yaml

import ieee_2030_5.models as m
from ieee_2030_5.utils import dataclass_to_xml


DEFAULT_URL  = "http://192.168.149.137:5002"
DEFAULT_CERT = "/home/engine/tls/combined/admin-combined.pem"
DEFAULT_ENDPOINT = "/derms/events"


# ---------------------------------------------------------------------------
# Type helpers
# ---------------------------------------------------------------------------

def _unwrap_optional(annotation) -> type:
    """
    Strip None from  X | None  or  Optional[X]  and return the inner type.
    Returns the annotation unchanged if it is not optional.
    """
    # Python 3.10+ pipe syntax:  int | None
    if isinstance(annotation, types.UnionType):
        args = [a for a in annotation.__args__ if a is not type(None)]
        return args[0] if args else annotation
    # typing.Optional[X] / typing.Union[X, None]
    if get_origin(annotation) is Union:
        args = [a for a in get_args(annotation) if a is not type(None)]
        return args[0] if args else annotation
    return annotation


def _list_item_type(annotation) -> type | None:
    """If the annotation is list[X], return X, otherwise return None."""
    if get_origin(annotation) is list:
        args = get_args(annotation)
        return args[0] if args else None
    return None


# ---------------------------------------------------------------------------
# Core conversion
# ---------------------------------------------------------------------------

def dict_to_dataclass(cls, data: dict):
    """
    Recursively convert a plain dict into a dataclass instance of cls.

    Handles:
      - Nested dicts    -> nested dataclasses  (e.g. DERControlBase, interval)
      - list of dicts   -> list of dataclasses  (e.g. CurveData)
      - hex strings     -> bytes               (e.g. mRID, lFDI, deviceCategory)
      - Unknown keys are silently ignored so YAML can carry comments/extras.
    """
    try:
        hints = get_type_hints(cls, localns=vars(m))
    except Exception:
        hints = {}

    valid_params = inspect.signature(cls).parameters
    kwargs = {}

    for key, value in data.items():
        if key not in valid_params:
            continue

        raw_annotation  = hints.get(key, type(value))
        inner_type      = _unwrap_optional(raw_annotation)
        list_item_cls   = _list_item_type(inner_type)

        if isinstance(value, dict) and dataclasses.is_dataclass(inner_type):
            # e.g.  interval: {duration: 3600, start: null}
            kwargs[key] = dict_to_dataclass(inner_type, value)

        elif isinstance(value, list) and list_item_cls is not None:
            if dataclasses.is_dataclass(list_item_cls):
                # e.g.  CurveData: [{xvalue: 100, yvalue: 50}, ...]
                kwargs[key] = [dict_to_dataclass(list_item_cls, item)
                               for item in value]
            else:
                kwargs[key] = value

        elif inner_type is bytes and isinstance(value, str):
            # e.g.  mRID: "A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4"
            kwargs[key] = bytes.fromhex(value)

        else:
            kwargs[key] = value

    return cls(**kwargs)


def build_event(entry: dict):
    """
    Pop the 'type' key, resolve the model class, fill in sensible
    defaults for required timing fields, then build the dataclass.
    """
    # entry is mutated so take a copy to avoid modifying the caller's dict
    entry = dict(entry)

    type_name = entry.pop("type")
    cls = getattr(m, type_name)

    now = int(time.time())

    # Fill creationTime if absent (required by Event)
    entry.setdefault("creationTime", now)

    # Fill interval.start with current time if left as null in YAML
    if "interval" in entry and isinstance(entry["interval"], dict):
        entry["interval"].setdefault("start", now)
        if entry["interval"]["start"] is None:
            entry["interval"]["start"] = now

    return dict_to_dataclass(cls, entry)


# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------

def post_event(obj, url: str, cert: str) -> requests.Response:
    xml = dataclass_to_xml(obj)
    return requests.post(
        url,
        data=xml,
        headers={"Content-Type": "application/sep+xml"},
        cert=cert,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Send IEEE 2030.5 events from a YAML file"
    )
    parser.add_argument("yml_file",
                        help="Path to the YAML events file")
    parser.add_argument("--url",      default=DEFAULT_URL,
                        help="Base server URL")
    parser.add_argument("--cert",     default=DEFAULT_CERT,
                        help="Path to client certificate PEM")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT,
                        help="Server endpoint path")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Print XML only, do not send")
    opts = parser.parse_args()

    data = yaml.safe_load(Path(opts.yml_file).read_text())
    url  = f"{opts.url}{opts.endpoint}"

    for i, entry in enumerate(data["events"]):
        print(f"\n--- Event {i + 1}: {entry.get('type')} ---")
        try:
            obj = build_event(entry)
            xml = dataclass_to_xml(obj)

            if opts.dry_run:
                print(xml)
                continue

            response = post_event(obj, url, opts.cert)
            print("Status  :", response.status_code)
            print("Response:", response.text)

        except Exception as e:
            print(f"ERROR building/sending event {i + 1}: {e}")
            raise


if __name__ == "__main__":
    main()
