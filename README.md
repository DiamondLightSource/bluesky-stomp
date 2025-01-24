[![CI](https://github.com/DiamondLightSource/bluesky-stomp/actions/workflows/ci.yml/badge.svg)](https://github.com/DiamondLightSource/bluesky-stomp/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/DiamondLightSource/bluesky-stomp/branch/main/graph/badge.svg)](https://codecov.io/gh/DiamondLightSource/bluesky-stomp)
[![PyPI](https://img.shields.io/pypi/v/bluesky-stomp.svg)](https://pypi.org/project/bluesky-stomp)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

# Bluesky Stomp

STOMP integration for bluesky

Source          | <https://github.com/DiamondLightSource/bluesky-stomp>
:---:           | :---:
PyPI            | `pip install bluesky-stomp`
Releases        | <https://github.com/DiamondLightSource/bluesky-stomp/releases>

## Low Level API

The library comes with some helpers for interacting with a stomp broker:

```python
from bluesky_stomp.messaging import MessageContext, StompClient
from bluesky_stomp.models import Broker, MessageQueue, MessageTopic

# Assumes you have an unauthenticated broker such as ActiveMQ running on localhost:61613
client = StompClient.for_broker(Broker(host="localhost", port=61613))

try:
    # Connect to the broker
    client.connect()

    # Send a message to a queue and a topic
    client.send(MessageQueue(name="my-queue"), {"foo": 1, "bar": 2})
    client.send(MessageTopic(name="my-topic"), {"foo": 1, "bar": 2})

    # Subscribe to messages on a topic, print all messages received,
    # assumes there is another service to post messages to the topic
    def on_message(message: str, context: MessageContext) -> None:
        print(message)

    client.subscribe(MessageTopic(name="my-other-topic"), on_message)

    # Send a message and wait for a reply, assumes there is another service
    # to post the reply
    reply_future = client.send_and_receive(
        MessageQueue(name="my-queue"), {"foo": 1, "bar": 2}
    )
    print(reply_future.result(timeout=5.0))
finally:
    # Disconnect at the end
    client.disconnect()
```
python -m bluesky_stomp --version
```
