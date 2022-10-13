FROM python:3.9

RUN mkdir /navigator-data-ingest
WORKDIR /navigator-data-ingest

# Install pip and poetry
RUN pip install --upgrade pip
RUN pip install "poetry==1.2.0"

# Copy files to image
COPY . .

# Install python dependencies using poetry
RUN poetry config virtualenvs.create false
RUN poetry install --without=dev

ENTRYPOINT [ "navigator-data-ingest" ]
