import json
import logging
import logging.config
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import click

from base.actions import handle_all_documents
from base.types import DocumentParserInput
from origins.cclw.load import CCLWDocumentCSV


DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
    },
    "loggers": {
        "": {  # root logger
            "handlers": ["default"],
            "level": "INFO",
        },
        "__main__": {  # if __name__ == '__main__'
            "handlers": ["default"],
            "level": "DEBUG",
            "propagate": False,
        },
    },
}

logging.config.dictConfig(DEFAULT_LOGGING)

_LOGGER = logging.getLogger(__file__)


@click.command()
@click.option("--input", help="Location of CSV input file.")
@click.option(
    "--output-location",
    help="Location in which to place output files for consumption by parsing stage.",
)
def main(input: str, output_location: str):
    """
    ETL for policy data.

    Extract policy data from known CCLW data source (and later this will be event-driven)
    Transform the policy data into parsed logical document groups.
    Load the policy data into our backend.

    :return None:
    """
    # load CCLW document descriptions from the given CSV file
    # TODO: command line args
    # TODO: more document sources
    # TODO: cloudpathlib
    csv_file_path = Path(input).absolute()
    output_location_path = Path(output_location)
    _LOGGER.info(f"Loading CCLW CSV data from '{csv_file_path}'")

    document_generator = CCLWDocumentCSV(csv_file_path)
    # TODO: configure worker count
    with ProcessPoolExecutor(max_workers=4) as executor:
        for parser_input in handle_all_documents(executor, document_generator):
            _write_parser_input(output_location_path, parser_input)


def _write_parser_input(
    output_location: Path, parser_input: DocumentParserInput
) -> None:
    output_file_location = output_location / f"{parser_input.import_id}.json"
    with open(output_file_location, "w") as output_file:
        output_file.write(json.dumps(parser_input.dict(), indent=2))


if __name__ == "__main__":

    if os.getenv("ENV") != "production":
        # for running locally (outside docker)
        from dotenv import load_dotenv

        load_dotenv("../../.env")
        load_dotenv("../../.env.local")

    main()
