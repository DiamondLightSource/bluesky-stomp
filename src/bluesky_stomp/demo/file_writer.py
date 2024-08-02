import logging
from pathlib import Path

from event_model import DocumentNames, RunRouter
from event_model.documents import RunStart
from suitcase.csv import Serializer

from bluesky_stomp.consumer import StompDocumentConsumer
from bluesky_stomp.messaging.stomptemplate import Queue
from bluesky_stomp.models import NamedDocument

logging.basicConfig(level=logging.INFO)

OUTPUT_PATH = Path("/tmp/bluesky-docs")
if not OUTPUT_PATH.exists():
    raise KeyError(f"{OUTPUT_PATH}: No such file or directory")

logging.info(f"Writing to {OUTPUT_PATH}")


def factory(name: str, start_doc: RunStart):
    serializer = Serializer(OUTPUT_PATH)
    return [serializer], []


router = RunRouter([factory])


def on_document(document: NamedDocument) -> None:
    router(document.name.value, document.doc)
    logging.debug(f"New document of type {document.name}")
    if document.name == DocumentNames.stop:
        uid = document.doc["uid"]
        logging.info(f"Completed run {uid}")


consumer = StompDocumentConsumer(Queue(name="documents"), on_document)

consumer.start()

input("Press enter to halt\n")
