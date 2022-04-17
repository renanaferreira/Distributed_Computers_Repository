"""CD Chat server program."""
import logging

import socket
import selectors

logging.basicConfig(filename="server.log", level=logging.DEBUG)

from .protocol import (
    CDProto,
    CDProtoBadFormat,
    JoinMessage,
    RegisterMessage,
    TextMessage,
)

MAIN_CHANNEL = -1


class Server:
    """Chat Server process."""

    def __init__(self, host: str = "localhost", port: int = 5010):
        """Initializes Server process."""
        self.address = (host, port)
        self.users = dict()
        self.channels = {MAIN_CHANNEL: set()}

        self.socket = socket.socket()
        self.selector = selectors.DefaultSelector()

        self.socket.bind((host, port))
        self.socket.listen(100)
        self.selector.register(self.socket, selectors.EVENT_READ, self.accept)

        logging.debug("Server %s initialized", self.address)

    def accept(self, sock):
        conn, addr = sock.accept()
        conn.setblocking(False)
        self.selector.register(conn, selectors.EVENT_READ, self.read)

        logging.debug("Server %s accepts conn %s from %s", self.address, conn, addr)

    def read(self, conn):
        msg = CDProto.recv_msg(conn)

        logging.debug("Server %s receives msg %s from conn %s", self.address, msg, conn)

        if msg:
            type_msg = type(msg)
            if type_msg == RegisterMessage:
                self.register_user(conn, msg.user)
            elif type_msg == JoinMessage:
                self.join_channel(conn, msg.channel)
            elif type_msg == TextMessage:
                channel = msg.channel
                if channel is None:
                    channel = MAIN_CHANNEL
                self.send_text(channel, msg.message)
            else:
                raise CDProtoBadFormat()
        else:
            self.unregister_user(conn)

    def register_user(self, sock, username):
        self.users[sock] = username
        self.channels[MAIN_CHANNEL].add(username)
        logging.debug(
            "Server %s registers user %s in main channel", self.address, username
        )

    def unregister_user(self, sock):
        self.selector.unregister(sock)
        user = self.users.pop(sock)
        for channel in self.channels:
            if user in self.channels[channel]:
                self.channels[channel].remove(user)
        for channel in [
            channel
            for channel in self.channels
            if channel == MAIN_CHANNEL or len(self.channels[channel]) > 0
        ]:
            if channel not in self.channels:
                self.channels.pop(channel)
        sock.close()
        logging.debug("Server %s unregisters user %s", self.address, user)

    def join_channel(self, sock, new_channel):
        username = self.users[sock]

        for channel in self.channels:
            if username in self.channels[channel]:
                self.channels[channel].remove(username)
                break

        if new_channel not in self.channels:
            self.channels[new_channel] = set()
        self.channels[new_channel].add(username)

        logging.debug("Server registers user %s in channel %s", username, new_channel)

    def send_text(self, channel, text):
        if channel in self.channels:
            socks = self.get_socks(self.channels[channel])
            for sock in socks:
                CDProto.send_msg(sock, CDProto.message(text, channel))

        logging.debug(
            "Server %s sends message %s in channel %s", self.address, text, channel
        )

    def get_socks(self, users):
        socks = set()
        for sock in self.users:
            if self.users[sock] in users:
                socks.add(sock)
        return socks

    def loop(self):
        """Loop indefinetely."""

        logging.debug("Server %s enters loop", self.address)

        while True:
            for key, mask in self.selector.select():
                callback = key.data
                callback(key.fileobj)
