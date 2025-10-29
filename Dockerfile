FROM python:3.10-slim-bullseye

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libreoffice \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /navigator-data-ingest
WORKDIR /navigator-data-ingest

# Install pip and poetry
RUN pip install --no-cache --upgrade pip
RUN pip install --no-cache "poetry==1.8.2"

# Copy poetry files to image
COPY poetry.lock ./poetry.lock
COPY pyproject.toml ./pyproject.toml

# Install python dependencies using poetry
RUN poetry config virtualenvs.create false
RUN poetry install --no-root

# Install playwright
RUN poetry run playwright install
RUN poetry run playwright install-deps

# Copy files to image
COPY src ./src
RUN poetry install

ENTRYPOINT [ "navigator-data-ingest" ]
