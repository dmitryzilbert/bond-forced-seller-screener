.PHONY: install run test lint

install:
python -m pip install -e ".[dev]"

run:
python -m app.main

test:
pytest -q

lint:
python -m compileall app
