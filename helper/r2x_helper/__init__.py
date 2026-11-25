"""r2x spike helper (stand-in for the framework repository's helper, RX-35).

Turns process.yml plus the values passed into a run into plain values.
It only reads values: it never reads or writes data, writes logs, runs checks or starts steps.

Framework run: the framework passes request_id and every value as plain run parameters
(input_<name>, input_<name>_version, output_<name>, setting_<name>).
Hand run (no request_id passed): values come from the sandbox section of process.yml.
"""
import os
from dataclasses import dataclass

import yaml

__version__ = "0.2.0"  # 0.2.0: versions live in requirements.txt, so process.yml no longer names the helper
KNOWN_FORMATS = {1}


@dataclass
class Values:
    source: str      # "framework" or "sandbox"
    request_id: str
    inputs: dict     # name -> {"table": str, "version": str or None}
    outputs: dict    # name -> table to write
    settings: dict   # name -> value, typed like its default in process.yml
    process_file: str
    helper_version: str = __version__

    def input_ref(self, name):
        """Table text to put after FROM, with the pinned version if one was given."""
        i = self.inputs[name]
        v = i.get("version")
        return f"{i['table']} VERSION AS OF {int(v)}" if v not in (None, "") else i["table"]


def find_process_file(start=None):
    """Walk up from the current folder to the first process.yml."""
    d = os.path.abspath(start or os.getcwd())
    while True:
        p = os.path.join(d, "process.yml")
        if os.path.isfile(p):
            return p
        parent = os.path.dirname(d)
        if parent == d:
            raise FileNotFoundError(f"process.yml not found in {start or os.getcwd()} or any folder above it")
        d = parent


def _passed_values():
    try:
        from databricks.sdk.runtime import dbutils
    except Exception:
        return lambda name: None

    def get(name):
        try:
            v = dbutils.widgets.get(name)
            return v if v != "" else None
        except Exception:
            return None
    return get


def _like(default, text):
    """Give a passed (text) setting the same type as its default."""
    if isinstance(default, bool):
        return text.strip().lower() in ("true", "1", "yes")
    if isinstance(default, int):
        return int(text)
    if isinstance(default, float):
        return float(text)
    return text


def get_values(process_file=None, passed=None):
    path = process_file or find_process_file()
    with open(path) as f:
        spec = yaml.safe_load(f) or {}
    if spec.get("format") not in KNOWN_FORMATS:
        raise ValueError(f"{path}: format {spec.get('format')!r} is not one this helper reads ({sorted(KNOWN_FORMATS)})")

    inputs = spec.get("inputs") or {}
    outputs = spec.get("outputs") or {}
    settings = spec.get("settings") or {}
    get = passed or _passed_values()
    request_id = get("request_id")

    if request_id:
        missing = []

        def need(name):
            v = get(name)
            if v is None:
                missing.append(name)
            return v
        vals = Values(
            source="framework", request_id=request_id,
            inputs={n: {"table": need(f"input_{n}"), "version": get(f"input_{n}_version")} for n in inputs},
            outputs={n: need(f"output_{n}") for n in outputs},
            settings={n: (_like(d, get(f"setting_{n}")) if get(f"setting_{n}") is not None else d) for n, d in settings.items()},
            process_file=path)
        if missing:
            raise ValueError("Framework run is missing passed values: " + ", ".join(missing))
        return vals

    sb = spec.get("sandbox") or {}
    sb_in, sb_out = sb.get("inputs") or {}, sb.get("outputs") or {}
    missing = [f"sandbox.inputs.{n}" for n in inputs if n not in sb_in] + [f"sandbox.outputs.{n}" for n in outputs if n not in sb_out]
    if missing:
        raise ValueError(f"{path}: sandbox section is missing " + ", ".join(missing))
    return Values(
        source="sandbox", request_id=str(sb.get("request_id", "manual-try")),
        inputs={n: {"table": sb_in[n]["table"], "version": None if sb_in[n].get("version") is None else str(sb_in[n]["version"])} for n in inputs},
        outputs={n: sb_out[n] for n in outputs},
        settings={n: (sb.get("settings") or {}).get(n, d) for n, d in settings.items()},
        process_file=path)
