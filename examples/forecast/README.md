# forecasting with an ONNX model

A complete loop around the `infer` transform: train a forecaster in Python,
run it inside a kayak pipeline, consume its output, and swap the model while
the pipeline runs.

```
service.py (feeder) ──POST──▶ pump_forecast ──http output──▶ service.py (receiver)
                              http input
                              infer (window of 24, 6 steps ahead)
```

- `pumps.py` — the simulated process: three pumps, a cycle and noise.
- `train.py` — fits a linear autoregression and writes it as ONNX into
  `example_config/models/`. Both `pump_forecast_v1.onnx` and
  `pump_forecast_v2.onnx` are committed, so nothing needs training to try it.
- `service.py` — posts a reading per pump every half second, receives what
  the pipeline emits, and scores each forecast against the reading it
  predicted, per model.
- `promote.py` — points the running pipeline at another model file.

Everything runs with [uv](https://docs.astral.sh/uv/). `service.py` and
`promote.py` need nothing outside the standard library.

## run it

```bash
just dev
```

```bash
cd examples/forecast && uv run service.py
```

After twelve seconds (the window has to fill) the service prints something
like this every five seconds:

```
330 observed, 1872 forecast points received
  models/pump_forecast_v1.onnx (ecdb920fcc3b): MAE 2.12 over the last 300
```

## swap the model

```bash
cd examples/forecast && uv run promote.py --user niclas models/pump_forecast_v2.onnx
```

`just dev` signs in with the users in `example_config/server.yaml`; give
`--user` one with the admin role, and its password in `KAYAK_PASSWORD` or at
the prompt. Against a server without sign-in, leave `--user` out.

The next report scores both: the v1 forecasts still waiting for their
readings, and the v2 forecasts made after the swap, at roughly half the error.

What happens during the swap:

1. kayak loads, optimises and trial-runs v2 **before** it stops anything. A
   model that cannot run (wrong shape, unsupported operator, not a model) is
   a 422, and v1 keeps running.
2. The old run loop stops and the new one starts under the same id. Posts
   that arrive in that moment get a 503. The feeder logs them and goes on.
3. The windows are in the `pump_windows` state bucket, not in the transform,
   so v2 forecasts from its first reading, without a twelve-second warm-up.

Nothing is written to disk. Save the config from the UI when v2 has earned
it. To roll back, promote v1 again. Both files stay on disk, and that is the
rule: **give a new model a new file name**. If you overwrite the file in place
and the new model is broken, the fallback reads the same broken file.

## what the consumer sees

The pipeline uses `emit: points`, so each batch the receiver gets holds both
kinds of message. `mark` tells them apart, and the forecast keeps the field
name `flow`, so a consumer reads one field and filters on `origin`:

```json
{"pump": "p1", "time": 1793640000000, "flow": 51.2, "origin": "observed"}
{"pump": "p1", "time": "2026-11-02T21:20:00.500Z", "step": 1, "flow": 51.9,
 "origin": "forecast", "model": {"path": "models/pump_forecast_v1.onnx", "sha256": "ecdb…"}}
```

`model` (from `provenance`) is what lets the receiver score each model on its
own, and what lets anyone ask later which model produced a forecast.

## retrain

```bash
cd examples/forecast && uv run train.py --version v3 --readings 50000 --ridge 0.5
```

then promote `models/pump_forecast_v3.onnx`. Any model with one float input
of shape `[1, 24, 1]` and one float output of shape `[1, horizon]` fits the
pipeline as configured.
