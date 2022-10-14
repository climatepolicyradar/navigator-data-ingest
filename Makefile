git_hooks:
	# Install git pre-commit hooks
	poetry run pre-commit install --install-hooks

build:
	docker build -t navigator-data-ingest .

