git_hooks:
	# Install git pre-commit hooks
	poetry run pre-commit install --install-hooks

build:
	docker build -t navigator-data-ingest .

test:
	docker run --entrypoint pytest navigator-data-ingest ./src -vvv --log-cli-level=INFO -m 'not integration'

setup:  ## Set up development environment (installs LibreOffice, Python deps, and Playwright)
	@command -v soffice >/dev/null 2>&1 || { \
		echo "Installing LibreOffice..."; \
		if [[ "$$OSTYPE" == "darwin"* ]]; then \
			brew install --cask libreoffice; \
		else \
			sudo apt-get update && sudo apt-get install -y libreoffice; \
		fi; \
	}
	poetry install
