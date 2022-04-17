"""CD Chat client program"""
import logging
import sys

import socket
import selectors
import fcntl
import os

from .protocol import CDProto, CDProtoBadFormat, TextMessage

logging.basicConfig(filename=f"{sys.argv[0]}.log", level=logging.DEBUG)

orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)


class Client:
    """Chat Client process."""

    def __init__(
        self, name: str = "Foo", server_host: str = "localhost", server_port: int = 5010
    ):
        """Initializes chat client."""
        self.name = name
        self.server = (server_host, server_port)
        self.channel = None

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.selector = selectors.DefaultSelector()
        self.selector.register(sys.stdin, selectors.EVENT_READ, self.write_user)

        logging.debug("Client %s is initiated", self.name)

    def connect(self):
        """Connect to chat server and setup stdin flags."""
        self.socket.connect(self.server)
        self.selector.register(self.socket, selectors.EVENT_READ, self.read)

        CDProto.send_msg(self.socket, CDProto.register(self.name))

        logging.debug("Client %s connected to server %s", self.name, self.server)

    def read(self, conn):
        logging.debug("Client %s reads socket", self.name)

        msg = CDProto.recv_msg(self.socket)

        if type(msg) == TextMessage:
            print(msg.message)

            logging.debug(
                "Client %s receives message %s in channel %s",
                self.name,
                msg.message,
                self.channel,
            )

    def write_user(self, conn):

        logging.debug("Client %s keyboard input", self.name)

        for line in conn:
            line = line.rstrip()
            if line == "exit":
                self.exit()
            args = line.split()
            if args[0] == "/join":
                self.join_channel(args[1])
            else:
                self.send_text(" ".join(args))

    def exit(self):
        logging.debug("Client %s is exiting", self.name)

        CDProto.send_msg(self.socket, CDProto.message("", self.channel))
        self.socket.close()
        sys.exit()

    def join_channel(self, channel):
        logging.debug("Client %s wants to join channel %s", self.name, self.channel)
        self.channel = channel
        CDProto.send_msg(self.socket, CDProto.join(self.channel))

    def send_text(self, text):
        logging.debug(
            "Client %s sends message %s to channel %s", self.name, text, self.channel
        )
        CDProto.send_msg(self.socket, CDProto.message(text, self.channel))

    def loop(self):
        """Loop indefinetely."""

        logging.debug("Client %s enters loop", self.name)

        while True:
            for key, mask in self.selector.select():
                callback = key.data
                callback(key.fileobj)
