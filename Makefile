.PHONY: test demo contracts lint typecheck package

test:
	PYTHONPATH=src python3 -m unittest discover -s tests -v

demo:
	PYTHONPATH=src python3 -m argo_dt.cli demo

contracts:
	PYTHONPATH=src python3 scripts/validate_contracts.py

lint:
	python3 -m ruff check src tests

typecheck:
	python3 -m mypy src

package:
	python3 -m build
