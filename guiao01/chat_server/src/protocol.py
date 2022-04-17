"""Protocol for chat server - Computação Distribuida Assignment 1."""
import json
from datetime import datetime
from socket import socket

ENCODING = "UTF-8"

class Message:
    """Message Type."""

    def __init__(self, command):
        self.command = command


class JoinMessage(Message):
    """Message to join a chat channel."""

    def __init__(self, command, channel):
        self.channel = channel
        super().__init__(command)

    def __repr__(self):
        return json.dumps({"command": self.command, "channel": self.channel})


class RegisterMessage(Message):
    """Message to register username in the server."""

    def __init__(self, command, user):
        self.user = user
        super().__init__(command)

    def __repr__(self):
        return json.dumps({"command": self.command, "user": self.user})


class TextMessage(Message):
    """Message to chat with other clients."""

    def __init__(self, command, message, channel):
        self.message = message
        self.channel = channel
        super().__init__(command)

    def __repr__(self):
        repr = {
            "command": self.command,
            "message": self.message,
            "ts": int(datetime.now().timestamp()),
        }
        if self.channel != None:
            repr["channel"] = self.channel
        return json.dumps(repr)


class CDProto:
    """Computação Distribuida Protocol."""

    @classmethod
    def register(cls, username: str) -> RegisterMessage:
        """Creates a RegisterMessage object."""
        return RegisterMessage("register", username)

    @classmethod
    def join(cls, channel: str) -> JoinMessage:
        """Creates a JoinMessage object."""
        return JoinMessage("join", channel)

    @classmethod
    def message(cls, message: str, channel: str = None) -> TextMessage:
        """Creates a TextMessage object."""
        return TextMessage("message", message, channel)

    @classmethod
    def send_msg(cls, connection: socket, msg: Message):
        """Sends through a connection a Message object."""
        rawdata = repr(msg).encode(ENCODING)
        header = len(rawdata).to_bytes(2, "big")
        connection.send(header + rawdata)

    @classmethod
    def recv_msg(cls, connection: socket) -> Message:
        """Receives through a connection a Message object."""
        try:
            size = int.from_bytes(connection.recv(2), "big")
            if size:
                msg = json.loads(
                    connection.recv(size).decode(ENCODING)
                )

                command = msg["command"]
                if command == "register":
                    return CDProto.register(msg["user"])
                elif command == "join":
                    return CDProto.join(msg["channel"])
                elif command == "message":
                    if msg["message"] == "":
                        return None
                    if "channel" in msg:
                        return CDProto.message(msg["message"], msg["channel"])
                    else:
                        return CDProto.message(msg["message"])
                else:
                    raise CDProtoBadFormat()
            else:
                return None
        except:
            raise CDProtoBadFormat()


class CDProtoBadFormat(Exception):
    """Exception when source message is not CDProto."""

    def __init__(self, original_msg: bytes = None):
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")
