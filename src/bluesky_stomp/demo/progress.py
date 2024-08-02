import logging

from bluesky_stomp.consumer import StompWaitingHookConsumer
from bluesky_stomp.messaging.stomptemplate import Queue
from bluesky_stomp.models import WaitingHookEvent


def print_statuses(event: WaitingHookEvent) -> None:
    print([f"{status.display_name}: {status.current}" for status in event.statuses.values()])

consumer = StompWaitingHookConsumer(Queue(name="status"), print_statuses)
consumer.start()

input("Press enter to halt\n")
