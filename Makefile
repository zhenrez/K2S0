.PHONY: test demo contracts grpc-generate benchmark-sync benchmark-epistemic soak-sync lint typecheck package

test:
	PYTHONPATH=src python3 -m unittest discover -s tests -v

demo:
	PYTHONPATH=src python3 -m argo_dt.cli demo

contracts:
	PYTHONPATH=src python3 scripts/validate_contracts.py

grpc-generate:
	python3 scripts/generate_grpc.py --output build/grpc

benchmark-sync:
	PYTHONPATH=src python3 scripts/benchmark_sync.py

benchmark-epistemic:
	PYTHONPATH=src python3 scripts/benchmark_epistemic.py

soak-sync:
	PYTHONPATH=src python3 scripts/soak_sync.py --duration-seconds 60

lint:
	python3 -m ruff check src tests

typecheck:
	python3 -m mypy src

package:
	python3 -m build
