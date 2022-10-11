FROM python:3.9

RUN mkdir /app
WORKDIR /app

# Install pip and poetry
RUN pip install --upgrade pip
RUN pip install "poetry==1.1.13"

# Copy files to image
COPY src/ .

# Install python dependencies using poetry
RUN poetry config virtualenvs.create false
RUN poetry install

ENTRYPOINT [ "python3", "main.py" ]