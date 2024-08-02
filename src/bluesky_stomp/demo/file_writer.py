from pathlib import Path

from event_model import RunRouter
from event_model.documents import RunStart
from suitcase.csv import Serializer

from bluesky_stomp.consumer import StompDocumentConsumer
from bluesky_stomp.messaging.stomptemplate import Queue

OUTPUT_PATH = Path("/tmp/bluesky-docs")
if not OUTPUT_PATH.exists():
    raise KeyError(f"{OUTPUT_PATH}: No such file or directory")


def factory(name: str, start_doc: RunStart):
    serializer = Serializer(OUTPUT_PATH)
    return [serializer], []

router = RunRouter([factory])
consumer = StompDocumentConsumer(Queue(name="documents"), lambda doc: router(doc["name"], doc["doc"]))

consumer.start()

input("Press enter to halt\n")
