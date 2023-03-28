git_hooks:
	# Install git pre-commit hooks
	poetry run pre-commit install --install-hooks

build:
	docker build -t navigator-data-ingest .
	docker build -t navigator-data-ingest-staging .

build_test:
	docker build -t navigator-data-ingest-test .

test:
	docker run --entrypoint pytest navigator-data-ingest-test ./src -vvv --log-cli-level=INFO -m 'not integration'
