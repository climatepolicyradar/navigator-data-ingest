FROM python:3.10.17

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

# Copy package referenced in poetry files to image
COPY src/navigator_data_ingest/main.py ./src/navigator_data_ingest/main.py

# Install python dependencies using poetry
RUN poetry config virtualenvs.create false
RUN poetry install

# Install playwright
RUN poetry run playwright install
RUN poetry run playwright install-deps

# Copy files to image
COPY src ./src
COPY integration_tests ./integration_tests

ENTRYPOINT [ "navigator-data-ingest" ]
