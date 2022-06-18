from abc import ABC, abstractmethod
from socket import socket

import json
import pickle
import xml.etree.ElementTree as ET

import src.tokens as tokens


class Serializador(ABC):

    @classmethod
    def get_type(cls):
        raise NotImplementedError()

    def __init__(self, encoding=tokens.UTF8):
        self.encoding = encoding

    @abstractmethod
    def serialize(self, data: dict) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, data: bytes) -> dict:
        pass


class JsonSerializador(Serializador):

    @classmethod
    def get_type(cls):
        return "JSON"

    def __init__(self, encoding=tokens.UTF8):
        super().__init__(encoding)

    def serialize(self, data: dict) -> bytes:
        data = json.dumps(data)
        data = data.encode(self.encoding)
        return data

    def deserialize(self, data: bytes) -> dict:
        data = data.decode(self.encoding)
        data = json.loads(data)
        return data


class XmlSerializador(Serializador):

    @classmethod
    def get_type(cls):
        return "XML"

    def __init__(self, encoding=tokens.UTF8):
        super().__init__(encoding)

    def serialize(self, data: dict) -> bytes:
        msg = ET.Element(tokens.MESSAGE)

        ET.SubElement(msg, tokens.COMMAND).set(tokens.VALUE, data[tokens.COMMAND])

        args = data.get(tokens.ARGS)
        if args:
            args_elem = ET.SubElement(msg, tokens.ARGS)
            for arg_key in args:
                elem = ET.SubElement(args_elem, arg_key)
                elem.set(tokens.VALUE, str(args[arg_key]))
                elem.set(tokens.TYPE, str(type(args[arg_key])))

        rawdata = ET.tostring(msg, encoding=self.encoding)
        return rawdata

    def deserialize(self, data: bytes) -> dict:
        new_data = dict()
        xml_tree = ET.fromstring(data.decode(self.encoding))
        new_data[tokens.COMMAND] = xml_tree.find(tokens.COMMAND).attrib[tokens.VALUE]
        args_tree = xml_tree.find(tokens.ARGS)
        if args_tree:
            args_dict = {}
            for elem in args_tree:
                _type = elem.attrib[tokens.TYPE]
                value = elem.attrib[tokens.VALUE]
                if _type == str(type(list())):
                    value = list(value)
                elif _type == str(type(1)):
                    value = int(value)
                args_dict[elem.tag] = value
            new_data[tokens.ARGS] = args_dict
        return new_data


class PickleSerializador(Serializador):

    @classmethod
    def get_type(cls):
        return "PICKLE"

    def __init__(self, encoding=tokens.UTF8):
        super().__init__(encoding)

    def serialize(self, data: dict) -> bytes:
        return pickle.dumps(data)

    def deserialize(self, data: bytes) -> dict:
        return pickle.loads(data)


class Message(ABC):

    @classmethod
    def definition(cls):
        raise NotImplementedError()

    @property
    def data(self):
        message = {tokens.COMMAND: self.command}
        args = self.args()
        if args != {}:
            message[tokens.ARGS] = args
        return message

    @property
    def command(self):
        return self.__class__.definition()

    @abstractmethod
    def args(self):
        pass


class ConnectMessage(Message):

    @classmethod
    def definition(cls):
        return "CONNECT"

    def __init__(self, serializer):
        self.serializer = serializer

    def args(self):
        return {tokens.SERIALIZER: self.serializer}


class PublishMessage(Message):

    @classmethod
    def definition(cls):
        return "PUBLISH"

    def __init__(self, topic, value):
        self.topic = topic
        self.value = value

    def args(self):
        return {tokens.TOPIC: self.topic, tokens.VALUE: self.value}


class SubscribeMessage(Message):

    @classmethod
    def definition(cls):
        return "SUBSCRIBE"

    def __init__(self, topic):
        self.topic = topic

    def args(self):
        return {tokens.TOPIC: self.topic}


class CancelSubscriptionMessage(Message):

    @classmethod
    def definition(cls):
        return "CANCEL_SUBSCRIPTION"

    def __init__(self, topic):
        self.topic = topic

    def args(self):
        return {tokens.TOPIC: self.topic}


class RequestListTopicsMessage(Message):

    @classmethod
    def definition(cls):
        return "REQUEST_LIST_TOPICS"

    def args(self):
        return {}


class ResponseListTopicsMessage(Message):

    @classmethod
    def definition(cls):
        return "RESPONSE_LIST_TOPICS"

    def __init__(self, list_topics):
        self.list_topics = list_topics

    def args(self):
        return {tokens.LIST_TOPICS: self.list_topics}


class PubSubProtocol:

    @classmethod
    def instantiate(cls, data: dict) -> Message:
        if not data:
            raise MessageBadFormat()
        command = data[tokens.COMMAND]
        args = data.get(tokens.ARGS)
        if command == ConnectMessage.definition():
            return ConnectMessage(args[tokens.SERIALIZER])
        elif command == PublishMessage.definition():
            return PublishMessage(args[tokens.TOPIC], args[tokens.VALUE])
        elif command == SubscribeMessage.definition():
            return SubscribeMessage(args[tokens.TOPIC])
        elif command == CancelSubscriptionMessage.definition():
            return CancelSubscriptionMessage(args[tokens.TOPIC])
        elif command == RequestListTopicsMessage.definition():
            return RequestListTopicsMessage()
        elif command == ResponseListTopicsMessage.definition():
            return ResponseListTopicsMessage(args[tokens.LIST_TOPICS])
        else:
            raise MessageBadFormat()

    @classmethod
    def send(cls, connection: socket, message: Message, serializer: Serializador):
        data = serializer.serialize(message.data)
        header = len(data).to_bytes(2, "big")
        rawdata = header + data
        connection.send(rawdata)

    @classmethod
    def recv(cls, connection: socket, serializer: Serializador) -> Message:
        try:
            size = int.from_bytes(connection.recv(2), "big")
            if size:
                rawdata = connection.recv(size)
                if rawdata:
                    data = serializer.deserialize(rawdata)
                    if data:
                        return PubSubProtocol.instantiate(data)
            else:
                return None
        except:
            raise MessageBadFormat()


class MessageBadFormat(Exception):
    """Exception when source message is not properly formatted."""

    def __init__(self, original_msg: bytes = None, encoding_method=tokens.UTF8):
        """Store original message that triggered exception."""
        self._original = original_msg
        self.encoding_method = encoding_method

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode(self.encoding_method)
