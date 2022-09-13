import logging
import logging.config
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from base.actions import handle_all_documents
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


def main():
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
    csv_file = Path(sys.argv[1]).absolute()
    _LOGGER.info(f"Loading CCLW CSV data from '{csv_file}'")

    document_generator = CCLWDocumentCSV(csv_file)
    # TODO: configure worker count
    with ProcessPoolExecutor(max_workers=4) as executor:
        handle_all_documents(executor, document_generator)


if __name__ == "__main__":

    if os.getenv("ENV") != "production":
        # for running locally (outside docker)
        from dotenv import load_dotenv

        load_dotenv("../../.env")
        load_dotenv("../../.env.local")

    main()
