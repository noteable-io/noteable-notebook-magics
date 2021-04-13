fmt: fmt-black fmt-isort

fmt-black:
	black .

fmt-isort:
	isort .

protoc: buf-gen fmt

buf-gen:
	buf generate

lint: lint-py lint-proto

lint-proto:
	buf lint

lint-py:
	black . --check
	isort . --check
	flake8
