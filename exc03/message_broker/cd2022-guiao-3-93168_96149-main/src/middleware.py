"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
from typing import Any
import src.tokens as tokens

import socket
from src.protocol import (
    Serializador,
    JsonSerializador,
    XmlSerializador,
    PickleSerializador,

    PubSubProtocol,

    ConnectMessage,
    PublishMessage,
    SubscribeMessage,
    CancelSubscriptionMessage,
    RequestListTopicsMessage,
    ResponseListTopicsMessage
)


SERVER = ("localhost", 5000)


class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER, serializer: Serializador = JsonSerializador()):
        """Create Queue."""
        self.topic = topic
        self.type = _type
        self.serializer = serializer
        self.server = SERVER

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect(self.server)
        PubSubProtocol.send(
            self.socket,
            ConnectMessage(self.serializer.__class__.get_type()),
            JsonSerializador()
        )

        if self.is_consumer:
            self.subscribed = False
            self.socket.setblocking(True)
        else:
            self.socket.setblocking(False)

    def push(self, value):
        """Sends data to broker."""
        is_producer = not self.is_consumer
        if is_producer:
            PubSubProtocol.send(
                self.socket,
                PublishMessage(self.topic, value),
                self.serializer
            )

    def pull(self) -> (str, Any):
        """Receives (topic, data) from broker.

        Should BLOCK the consumer!"""
        if self.is_consumer:
            if not self.subscribed:
                PubSubProtocol.send(self.socket, SubscribeMessage(self.topic), self.serializer)
                self.subscribed = True
            msg = PubSubProtocol.recv(self.socket, self.serializer)
            if msg.definition() == PublishMessage.definition():
                args = msg.args()
                return args[tokens.TOPIC], args[tokens.VALUE]
            else:
                return None, None
        else:
            return None, None

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""

        self.socket.setblocking(True)

        PubSubProtocol.send(
            self.socket,
            RequestListTopicsMessage(),
            self.serializer
        )

        msg = PubSubProtocol.recv(
            self.socket,
            self.serializer
        )

        if msg and msg.definition() == ResponseListTopicsMessage.definition():
            list_topics = msg.args()[tokens.LIST_TOPICS]
            callback(list_topics)

        if not self.is_consumer:
            self.socket.setblocking(False)

    def cancel(self):
        """Cancel subscription."""
        if self.is_consumer:
            PubSubProtocol.send(
                self.socket,
                CancelSubscriptionMessage(self.topic),
                self.serializer
            )
            self.subscribed = False

    @property
    def is_consumer(self):
        return self.type == MiddlewareType.CONSUMER


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type=_type, serializer=JsonSerializador())


class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type=_type, serializer=XmlSerializador())


class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type=_type, serializer=PickleSerializador())
