"""Generate Python gRPC modules into an isolated host-build directory."""

from __future__ import annotations

import argparse
import subprocess
import sys
from importlib.resources import files
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROTO = ROOT / "proto" / "argo" / "dt" / "v1" / "twin.proto"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "build" / "grpc")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    try:
        grpc_include = Path(str(files("grpc_tools") / "_proto"))
    except ModuleNotFoundError as exc:
        raise SystemExit("install the 'grpc' extra before generating stubs") from exc
    command = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{ROOT / 'proto'}",
        f"-I{grpc_include}",
        f"--python_out={output}",
        f"--pyi_out={output}",
        f"--grpc_python_out={output}",
        str(PROTO),
    ]
    completed = subprocess.run(command, check=False)
    if completed.returncode:
        raise SystemExit(completed.returncode)
    generated = output / "argo" / "dt" / "v1" / "twin_pb2_grpc.py"
    if not generated.is_file():
        raise SystemExit("gRPC generation completed without the expected module")
    print(f"generated gRPC Python modules under {output}")


if __name__ == "__main__":
    main()
