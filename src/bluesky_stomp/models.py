from pydantic import BaseModel, Field


class AuthenticationBase:
    """
    Base class for types of authentication that stomp should recognise
    """


class BasicAuthentication(AuthenticationBase, BaseModel):
    username: str = Field(description="Unique identifier for user")
    password: str = Field(description="Password to verify user's identity")


class DestinationBase:
    """Base class for possible destinations of stomp messages"""


class Queue(DestinationBase, BaseModel):
    """
    Represents a queue (unicast) on a stomp broker
    """

    name: str = Field(description="Name of message queue on broker")


class TemporaryQueue(DestinationBase, BaseModel):
    """
    Represents a temporary queue (unicast) on a stomp broker,
    the broker may delete the queue after use
    """

    name: str = Field(description="Name of message queue on broker")


class Topic(DestinationBase, BaseModel):
    """
    Represents a topic (multicast) on a stomp broker
    """

    name: str = Field(description="Name of message topic on broker")
