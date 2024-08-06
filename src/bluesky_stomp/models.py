from pydantic import BaseModel, ConfigDict, Field


class BasicAuthentication(BaseModel):
    """
    User credentials for basic authentication
    """

    username: str = Field(description="Unique identifier for user")
    password: str = Field(description="Password to verify user's identity")


class Broker(BaseModel):
    """
    Details required to connect to a message broker
    """

    host: str = Field(description="Host IP/DNS name")
    port: int = Field(description="Port intended for STOMP messages")
    auth: BasicAuthentication | None = Field(
        description="Authentication details, if required", default=None
    )

    @classmethod
    def localhost(cls) -> "Broker":
        return cls(host="localhost", port=61613)


class DestinationBase(BaseModel):
    """Base class for possible destinations of stomp messages"""

    model_config = ConfigDict(frozen=True)


class MessageQueue(DestinationBase):
    """
    Represents a queue (unicast) on a stomp broker
    """

    name: str = Field(description="Name of message queue on broker")


class TemporaryMessageQueue(DestinationBase):
    """
    Represents a temporary queue (unicast) on a stomp broker,
    the broker may delete the queue after use
    """

    name: str = Field(description="Name of message queue on broker")


class ReplyMessageQueue(DestinationBase):
    """
    Represents a temporary queue (unicast) on a stomp broker specifically
    for replying to other messages. Needed because RabbitMQ uses reply
    queues and ActiveMQ uses temp queues.
    """

    name: str = Field(description="Name of message queue on broker")


class MessageTopic(DestinationBase):
    """
    Represents a topic (multicast) on a stomp broker
    """

    name: str = Field(description="Name of message topic on broker")
