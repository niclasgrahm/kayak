"""Both ends of the demo pipeline, in one process and the standard library only.

The feeder posts a reading per pump every half second to the `pump_forecast`
pipeline's http input. The receiver is what that pipeline's http output
delivers to: the readings, each labelled `origin: observed`, interleaved with
the forecast points the model made from them, each labelled `origin:
forecast` and carrying the `model` (path and sha256) that made it.

The receiver does what a consumer of a forecast actually wants: it holds each
forecast until the reading it predicted arrives and scores it -- per model, so
a swap shows up as the error changing hands from one sha to the next.

    uv run service.py                       # kayak on :6767, receiver on :6768
    uv run service.py --kayak http://host:6767 --listen 0.0.0.0:6768
"""

import argparse
import json
import random
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict, deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from pumps import INTERVAL_SECONDS, PUMPS, flow

PIPELINE = "pump_forecast"


def feed(kayak: str, stop: threading.Event) -> None:
    url = f"{kayak}/api/pipelines/{PIPELINE}/messages"
    rng = random.Random()
    step = 0
    complained = False
    while not stop.is_set():
        now = int(time.time() * 1000)
        # whole half-seconds, so a forecast's time and the reading it
        # predicted are the same number
        now -= now % int(INTERVAL_SECONDS * 1000)
        batch = [{"pump": pump, "time": now, "flow": round(flow(pump, step, rng), 3)} for pump in PUMPS]
        request = urllib.request.Request(
            url, data=json.dumps(batch).encode(), headers={"content-type": "application/json"}, method="POST"
        )
        try:
            urllib.request.urlopen(request, timeout=5).close()
            complained = False
        except (urllib.error.URLError, OSError) as e:
            if not complained:
                print(f"feeder: {url}: {e} -- is kayak running with the example config?")
                complained = True
        step += 1
        stop.wait(INTERVAL_SECONDS)


def millis(value) -> int:
    """A time as the pipeline writes it: epoch millis on a reading, RFC 3339
    on a forecast point (kayak writes the points' times as text)."""
    if isinstance(value, (int, float)):
        return int(value)
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


class Scores:
    """Forecasts waiting for their reading, and the errors once it came."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        # (pump, time) -> [(model, step, predicted)]
        self.pending: dict[tuple[str, int], list[tuple[str, int, float]]] = defaultdict(list)
        # model -> recent absolute errors
        self.errors: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=300))
        self.models: dict[str, str] = {}
        self.counts = {"observed": 0, "forecast": 0}

    def take(self, messages: list[dict]) -> None:
        with self.lock:
            for m in messages:
                origin = m.get("origin")
                key = (m["pump"], millis(m["time"]))
                if origin == "forecast":
                    model = m["model"]["sha256"][:12]
                    self.models[model] = m["model"]["path"]
                    self.pending[key].append((model, m["step"], m["flow"]))
                    self.counts["forecast"] += 1
                elif origin == "observed":
                    for model, _step, predicted in self.pending.pop(key, []):
                        self.errors[model].append(abs(predicted - m["flow"]))
                    self.counts["observed"] += 1

    def report(self) -> str:
        with self.lock:
            lines = [f"{self.counts['observed']} observed, {self.counts['forecast']} forecast points received"]
            for model, errors in self.errors.items():
                if errors:
                    mae = sum(errors) / len(errors)
                    lines.append(f"  {self.models[model]} ({model}): MAE {mae:.2f} over the last {len(errors)}")
            return "\n".join(lines)


def receiver(scores: Scores):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 -- the stdlib's spelling
            body = self.rfile.read(int(self.headers.get("content-length", 0)))
            try:
                messages = json.loads(body)
                scores.take(messages if isinstance(messages, list) else [messages])
            except (ValueError, KeyError, TypeError) as e:
                self.send_error(400, str(e))
                return
            self.send_response(204)
            self.end_headers()

        def log_message(self, *_args) -> None:
            pass

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--kayak", default="http://127.0.0.1:6767")
    parser.add_argument("--listen", default="127.0.0.1:6768")
    args = parser.parse_args()

    host, port = args.listen.rsplit(":", 1)
    scores = Scores()
    server = ThreadingHTTPServer((host, int(port)), receiver(scores))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    stop = threading.Event()
    threading.Thread(target=feed, args=(args.kayak, stop), daemon=True).start()
    print(f"feeding {args.kayak}/api/pipelines/{PIPELINE}/messages, receiving on http://{args.listen}")
    try:
        while True:
            time.sleep(5)
            print(scores.report())
    except KeyboardInterrupt:
        stop.set()
        server.shutdown()


if __name__ == "__main__":
    main()
