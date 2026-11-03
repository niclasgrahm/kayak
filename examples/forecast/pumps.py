"""The pumps the demo pretends to read: shared by the trainer and the feeder,
so the model is trained on the same process it later forecasts.

Each pump's flow is a base level, a cycle of `PERIOD` readings and some noise.
The pumps differ in level, amplitude and phase, so one model has to forecast
all three from their own recent history -- which is what a window model does.
"""

import math
import random

# One reading every half second.
INTERVAL_SECONDS = 0.5
# The cycle, in readings: 30 seconds.
PERIOD = 60
# What the model reads and what it answers, in readings.
WINDOW = 24
HORIZON = 6

PUMPS = {
    "p1": {"base": 50.0, "amplitude": 8.0, "phase": 0.0},
    "p2": {"base": 80.0, "amplitude": 12.0, "phase": 1.3},
    "p3": {"base": 30.0, "amplitude": 5.0, "phase": 2.6},
}


def flow(pump: str, step: int, rng: random.Random, noise: float = 1.0) -> float:
    """The flow of `pump` at reading number `step`."""
    p = PUMPS[pump]
    cycle = math.sin(2 * math.pi * step / PERIOD + p["phase"])
    # a slower second harmonic, so a plain sine fit is not the whole answer
    wobble = 0.3 * math.sin(2 * math.pi * step / (PERIOD * 3.7))
    return p["base"] + p["amplitude"] * (cycle + wobble) + rng.gauss(0.0, noise)
