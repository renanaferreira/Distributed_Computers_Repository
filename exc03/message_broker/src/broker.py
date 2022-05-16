"""Message Broker"""
import enum
from typing import Dict, List, Any, Tuple
import socket
import selectors

from src.protocol import (
    JsonSerializador,
    XmlSerializador,
    PickleSerializador,

    PubSubProtocol,

    ConnectMessage,
    PublishMessage,
    SubscribeMessage,
    CancelSubscriptionMessage,
    RequestListTopicsMessage,
    ResponseListTopicsMessage,

    MessageBadFormat
)
from src.log import get_logger
from src.tree import Tree
import src.tokens as tokens


class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2


class Broker:
    """Implementation of a PubSub Message Broker."""

    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self._host = "localhost"
        self._port = 5000
        self.address = (self._host, self._port)
        self.logger = get_logger(f"Broker {self.address}")

        self.socket = socket.socket()
        self.socket.bind(self.address)
        self.socket.listen(100)

        self.selector = selectors.DefaultSelector()
        self.selector.register(self.socket, selectors.EVENT_READ, self.accept)

        # Instantiate serializers
        self.serializers = dict()
        self.serializers[Serializer.JSON] = JsonSerializador()
        self.serializers[Serializer.XML] = XmlSerializador()
        self.serializers[Serializer.PICKLE] = PickleSerializador()

        self.users = dict()
        self.topic_tree = Tree()
        self.topics_by_producers = set()

        self.logger.info("Initialized")

    def accept(self, sock):
        conn, addr = sock.accept()
        self.logger.info("Accept connection %s", conn)
        msg = PubSubProtocol.recv(conn, JsonSerializador())
        if msg.definition() == ConnectMessage.definition():
            serializer = self.get_serializer_type(msg.args()[tokens.SERIALIZER])
            if serializer:
                self.register_sock(conn, serializer)

    def read(self, sock):
        msg = PubSubProtocol.recv(
            sock,
            self.serializers[self.users[sock]]
        )
        if msg:
            self.logger.info(f"read msg {msg.data} from sock {sock}")
            if msg.command == PublishMessage.definition():
                topic = msg.args()[tokens.TOPIC]
                value = msg.args()[tokens.VALUE]
                self.logger.info(f"Publish in topic {topic} value {value}")
                self.put_topic(topic, value)
                for sock, serializer in self.list_subscriptions(topic):
                    self.logger.info(f"send published value to sock {sock} value {value} in topic {topic}")
                    PubSubProtocol.send(
                        sock,
                        PublishMessage(topic, value),
                        self.serializers[serializer]
                    )
            elif msg.command == SubscribeMessage.definition():
                self.subscribe(msg.args()[tokens.TOPIC], sock, _format=self.users[sock])
            elif msg.command == CancelSubscriptionMessage.definition():
                self.unsubscribe(msg.args()[tokens.TOPIC], sock)
            elif msg.command == RequestListTopicsMessage.definition():
                self.logger.info("Request list of topics")
                PubSubProtocol.send(
                    sock,
                    ResponseListTopicsMessage(self.list_topics()),
                    self.serializers[self.users[sock]]
                )
            else:
                raise MessageBadFormat()
        else:
            self.logger.info(f"message received is None")
            self.unregister_sock(sock)

    def register_sock(self, sock: socket.socket, serializer: Serializer):
        self.logger.info(f"Register sock {sock} with serializer {serializer}")
        sock.setblocking(False)
        self.selector.register(sock, selectors.EVENT_READ, self.read)
        self.users[sock] = serializer

    def unregister_sock(self, sock: socket.socket):
        self.logger.info(f"close sock {sock}")
        self.topic_tree.remove_subscriber(sock)
        self.selector.unregister(sock)
        self.users.pop(sock)

    def get_serializer_type(self, serializer):
        if serializer == JsonSerializador.get_type():
            return Serializer.JSON
        elif serializer == XmlSerializador.get_type():
            return Serializer.XML
        elif serializer == PickleSerializador.get_type():
            return Serializer.PICKLE
        else:
            return None

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""
        self.logger.info("list_topics()")
        #return self.topic_tree.get_list_topics()
        # TODO for testing purposes
        return list(self.topics_by_producers)

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        self.logger.info(f"get_topic by topic {topic}")
        return self.topic_tree.get_value(topic)

    def put_topic(self, topic, value):
        """Store in topic the value."""
        self.logger.info(f"put_topic by topic {topic} and value {value}")
        self.topic_tree.put_topic(topic, value)
        self.topics_by_producers.add(topic)

    def list_subscriptions(self, topic: str) -> List[Tuple[socket.socket, Serializer]]:
        """Provide list of subscribers to a given topic."""
        self.logger.info(f"subscriptions by topic {topic}")
        subscribers = list()
        for subscriber in self.topic_tree.list_subscriptions(topic):
            subscribers.append((subscriber, self.users[subscriber]))
        return subscribers

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        self.logger.info(f"subscribe {address} by topic {topic} with serializer {_format}")
        self.topic_tree.subscribe_topic(topic, address)
        # TODO for testing purposes
        self.users[address] = _format

    # TODO for testing purposes
    def add_subscriber(self, topic: str, address: socket.socket, serializer: Serializer):
        self.subscribe(topic, address, _format=serializer)

    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        self.logger.info(f"Cancel subscription of {address} in topic {topic}")
        self.topic_tree.unsubscribe_topic(topic, address)

    def run(self):
        """Run until canceled."""

        self.logger.info("run loop until canceled")

        while not self.canceled:
            for key, mask in self.selector.select():
                callback = key.data
                callback(key.fileobj)
