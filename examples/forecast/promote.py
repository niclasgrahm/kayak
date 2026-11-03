"""Swap the model the running `pump_forecast` pipeline uses, without a restart.

    uv run promote.py models/pump_forecast_v2.onnx
    uv run promote.py models/pump_forecast_v1.onnx     # and back again
    uv run promote.py --user niclas models/pump_forecast_v2.onnx   # a server with sign-in

With --user, the password is read from KAYAK_PASSWORD, or asked for.

Reads the config the pipeline runs (not the file on disk), points its infer
transform at the new model, and replaces the pipeline with it. kayak loads and
checks the new model before it stops anything, so a model that cannot run is
refused with the old one still running, and the forecast windows live in the
pipeline's state bucket, so the new model forecasts from its first reading.

Nothing is saved: save the config from the UI (or POST /api/config/save) once
the new model has earned it.
"""

import argparse
import base64
import getpass
import json
import os
import sys
import urllib.error
import urllib.request

PIPELINE = "pump_forecast"


def call(method: str, url: str, auth: str | None, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    headers = {"content-type": "application/json"}
    if auth:
        headers["authorization"] = auth
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {url}: {e.code} {e.read().decode(errors='replace')}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("model", help="the new model's path, relative to the config's directory")
    parser.add_argument("--kayak", default="http://127.0.0.1:6767")
    parser.add_argument("--user", help="sign in as this user (basic auth)")
    args = parser.parse_args()
    auth = None
    if args.user:
        password = os.environ.get("KAYAK_PASSWORD") or getpass.getpass(f"password for {args.user}: ")
        auth = "Basic " + base64.b64encode(f"{args.user}:{password}".encode()).decode()

    base = f"{args.kayak}/api/pipelines/{PIPELINE}"
    config = json.loads(call("GET", f"{base}/config?format=json", auth)["text"])
    infers = [t for t in config.get("transforms", []) if t.get("type") == "infer"]
    if len(infers) != 1:
        sys.exit(f"expected one infer transform on {PIPELINE}, found {len(infers)}")
    old = infers[0]["model"]["path"]
    infers[0]["model"] = {"type": "file", "path": args.model}
    call("PUT", base, auth, config)
    print(f"{PIPELINE}: {old} -> {args.model}")


if __name__ == "__main__":
    main()
