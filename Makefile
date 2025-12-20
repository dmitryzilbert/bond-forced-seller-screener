.PHONY: install run test lint

install:
pip install -e .[dev]

run:
python -m app.main

test:
pytest

lint:
python -m compileall app
