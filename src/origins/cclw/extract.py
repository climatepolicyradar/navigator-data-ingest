import logging
from csv import DictReader
from datetime import datetime
from html.parser import HTMLParser
from io import StringIO
from typing import Generator, Sequence

from src.base.types import (
    DEFAULT_POLICY_DATE,
    PUBLICATION_EVENT_NAME,
    Document,
    DocumentType,
    Event,
)

_LOGGER = logging.getLogger(__file__)

ACTION_ID_FIELD = "Id"
DOCUMENT_ID_FIELD = "Document Id"
TITLE_FIELD = "Title"
DESCRIPTION_FIELD = "Description"
COUNTRY_CODE_FIELD = "Geography ISO"
DOCUMENT_FIELD = "Documents"
CATEGORY_FIELD = "Category"
EVENTS_FIELD = "Events"
SECTORS_FIELD = "Sectors"
INSTRUMENTS_FIELD = "Instruments"
FRAMEWORKS_FIELD = "Frameworks"
TOPICS_FIELD = "Responses"
HAZARDS_FIELD = "Natural Hazards"
DOCUMENT_TYPE_FIELD = "Document Type"
YEAR_FIELD = "Year"
LANGUAGES_FIELD = "Language"
KEYWORDS_FIELD = "Keywords"
GEOGRAPHY_FIELD = "Geography"
PARENT_LEGISLATION_FIELD = "Parent Legislation"


def _split_not_null(input_string: str, split_char: str) -> Sequence[str]:
    return [s.strip() for s in input_string.strip().split(split_char) if s is not None]


def _extract_events(events_str: str) -> Sequence[Event]:
    events = []
    for event_string in _split_not_null(events_str, ";"):
        event_parts = event_string.split("|")
        date_str = event_parts[0]
        event_date = datetime.strptime(date_str, "%d/%m/%Y")
        event_name = event_parts[1]
        events.append(Event(name=event_name, description="", date=event_date))
    return events


def extract_documents(
    csv_reader: DictReader,
) -> Generator[Document, None, None]:
    for row in csv_reader:
        country_code = row[COUNTRY_CODE_FIELD].strip()
        year = row[YEAR_FIELD].strip()
        document_name = row[TITLE_FIELD].strip()
        document_description = _strip_tags(row[DESCRIPTION_FIELD])
        action_id = row[ACTION_ID_FIELD].strip()
        import_id = f"{action_id}-{row[DOCUMENT_ID_FIELD].strip()}"
        document_url = _parse_url(row[DOCUMENT_FIELD])
        document_languages = _split_not_null(row[LANGUAGES_FIELD], ";")
        document_category = DocumentType[row[CATEGORY_FIELD].strip()].value
        document_type = row[DOCUMENT_TYPE_FIELD].strip()
        events = _extract_events(row[EVENTS_FIELD])
        sectors = _split_not_null(row[SECTORS_FIELD], ";")
        instruments = _split_not_null(row[INSTRUMENTS_FIELD], ";")
        frameworks = _split_not_null(row[FRAMEWORKS_FIELD], ";")
        topics = _split_not_null(row[TOPICS_FIELD], ";")
        hazards = _split_not_null(row[HAZARDS_FIELD], ";")
        keywords = _split_not_null(row[KEYWORDS_FIELD], ";")

        publication_date = _calculate_publication_date(
            events=events,
            fallback_year=year,
        )

        yield Document(
            name=document_name,
            description=document_description,
            source_url=document_url,
            import_id=import_id,
            publication_ts=publication_date,
            url=None,  # Not yet uploaded
            md5sum=None,  # Calculated during upload
            content_type=None,  # Detected during upload
            languages=document_languages,
            type=document_type,
            source=document_url,
            category=document_category,
            geography=country_code,
            frameworks=frameworks,
            instruments=instruments,
            topics=topics,
            keywords=keywords,
            hazards=hazards,
            sectors=sectors,
            events=events,
        )


def _calculate_publication_date(
    events: Sequence[Event],
    fallback_year: str,
) -> datetime:
    """
    Calculate the publication date from a sequence of events and a given fallback year.

    Calculates the publication date of a document according to the following heuristic:
        - The date of a "Publication" event if present
        - The earliest event if no "Publication" event is found
        - The first of January on the given fallback year if no events are present
        - DEFAULT_POLICY_DATE if no other useful information can be derived

    A warning will be issued if the fallback_year does not match a discovered event.

    :param Sequence[Event] events: A sequence of parsed events associated with
        the document
    :returns datetime: The calculated publication date as described by the
        heuristic above
    """
    publication_date = None

    for event in events:
        if event.name.lower() == PUBLICATION_EVENT_NAME.lower():
            return event.date

        if publication_date is None or event.date < publication_date:
            publication_date = event.date

    if publication_date is not None and publication_date.year != fallback_year:
        _LOGGER.warn(
            f"Publication date '{publication_date.isoformat()}' does not "
            f"match given fallback year '{fallback_year}'"
        )

    if publication_date is None:
        try:
            parsed_fallback_year = int(fallback_year.strip())
        except ValueError:
            _LOGGER.exception(f"Could not parse fallback year '{fallback_year}'")
        else:
            publication_date = datetime(year=parsed_fallback_year, month=1, day=1)

    return publication_date or DEFAULT_POLICY_DATE


def _parse_url(url: str) -> str:
    """
    Parse a document URL.

    In addition to parsing the URL, we also:
        - convert http to https
        - Remove any delimiters (a hang-over from the original CSV)

    :param str url: An input string representing a URL
    :returns str: An updated parsed URL as a string
    """
    return url.split("|")[0].strip().replace("http://", "https://")


class _HTMLStripper(HTMLParser):
    """Strips HTML from strings."""

    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.text = StringIO()

    def handle_data(self, d):  # noqa:D102
        self.text.write(d)

    def get_data(self):  # noqa:D102
        return self.text.getvalue()


def _strip_tags(html: str) -> str:
    s = _HTMLStripper()
    s.feed(html)
    return s.get_data()
