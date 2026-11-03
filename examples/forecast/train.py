# /// script
# requires-python = ">=3.11"
# dependencies = ["numpy>=1.26", "onnx>=1.16"]
# ///
"""Train a pump flow forecaster and write it as an ONNX file kayak can run.

The model is a linear autoregression: the next `HORIZON` readings as a
weighted sum of the last `WINDOW`, fitted by ridge regression. That is a
deliberately small model -- the point is the round trip (train here, run in
kayak, swap without a restart), not the forecaster. Anything that exports to
ONNX with one float input and one float output takes the same path:
scikit-learn through skl2onnx, PyTorch through torch.onnx.export.

    uv run train.py --version v1 --readings 200 --ridge 20000
    uv run train.py --version v2 --readings 20000 --ridge 1

writes ../../example_config/models/pump_forecast_<version>.onnx. v1 is trained
on little data with heavy regularisation, so v2 is visibly better -- which is
what makes the swap in the README worth watching.

The graph is written by hand with onnx.helper so it is readable:

    readings [1, WINDOW, 1] -> Reshape [1, WINDOW] -> MatMul W -> Add b -> flow [1, HORIZON]

That is the `window` shape kayak's infer transform feeds: `[1, size, inputs]`
in, `[1, horizon]` out for one output field.
"""

import argparse
import pathlib
import random

import numpy as np
import onnx
from onnx import TensorProto, helper, numpy_helper

from pumps import HORIZON, PUMPS, WINDOW, flow

HERE = pathlib.Path(__file__).resolve().parent
MODELS = HERE.parent.parent / "example_config" / "models"


def dataset(readings: int, seed: int) -> tuple[np.ndarray, np.ndarray]:
    """Windows and what followed them, for every pump."""
    rng = random.Random(seed)
    xs, ys = [], []
    for pump in PUMPS:
        series = np.array([flow(pump, step, rng) for step in range(readings)], dtype=np.float64)
        for start in range(readings - WINDOW - HORIZON + 1):
            xs.append(series[start : start + WINDOW])
            ys.append(series[start + WINDOW : start + WINDOW + HORIZON])
    return np.array(xs), np.array(ys)


def fit(x: np.ndarray, y: np.ndarray, ridge: float) -> tuple[np.ndarray, np.ndarray]:
    """Ridge regression with an unpenalised intercept: centre, solve, add back."""
    x_mean, y_mean = x.mean(axis=0), y.mean(axis=0)
    xc, yc = x - x_mean, y - y_mean
    weights = np.linalg.solve(xc.T @ xc + ridge * np.eye(x.shape[1]), xc.T @ yc)
    bias = y_mean - x_mean @ weights
    return weights.astype(np.float32), bias.astype(np.float32)


def to_onnx(weights: np.ndarray, bias: np.ndarray, version: str) -> onnx.ModelProto:
    graph = helper.make_graph(
        nodes=[
            helper.make_node("Reshape", ["readings", "flat_shape"], ["flat"]),
            helper.make_node("MatMul", ["flat", "weights"], ["weighted"]),
            helper.make_node("Add", ["weighted", "bias"], ["flow"]),
        ],
        name="pump_forecast",
        inputs=[helper.make_tensor_value_info("readings", TensorProto.FLOAT, [1, WINDOW, 1])],
        outputs=[helper.make_tensor_value_info("flow", TensorProto.FLOAT, [1, HORIZON])],
        initializer=[
            numpy_helper.from_array(np.array([1, WINDOW], dtype=np.int64), "flat_shape"),
            numpy_helper.from_array(weights, "weights"),
            numpy_helper.from_array(bias, "bias"),
        ],
    )
    model = helper.make_model(
        graph,
        producer_name="kayak-example-train",
        producer_version=version,
        opset_imports=[helper.make_opsetid("", 13)],
    )
    model.ir_version = 8
    onnx.checker.check_model(model)
    return model


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--version", required=True, help="written into the file name, e.g. v1")
    parser.add_argument("--readings", type=int, default=20_000, help="readings of history per pump")
    parser.add_argument("--ridge", type=float, default=1.0, help="regularisation strength")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    x, y = dataset(args.readings, args.seed)
    weights, bias = fit(x, y, args.ridge)

    held_out_x, held_out_y = dataset(2_000, args.seed + 1)
    mae = np.abs(held_out_x @ weights + bias - held_out_y).mean(axis=0)

    MODELS.mkdir(parents=True, exist_ok=True)
    path = MODELS / f"pump_forecast_{args.version}.onnx"
    onnx.save(to_onnx(weights, bias, args.version), path)
    print(f"wrote {path.relative_to(HERE.parent.parent)}")
    print("held-out MAE by step ahead: " + "  ".join(f"t+{h + 1}: {e:.2f}" for h, e in enumerate(mae)))


if __name__ == "__main__":
    main()
