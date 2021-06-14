fmt: fmt-black fmt-isort

fmt-black:
	black .

fmt-isort:
	isort .

lint: lint-py

lint-py:
	black . --check
	isort . --check
	flake8
