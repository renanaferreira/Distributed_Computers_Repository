# coding: utf-8

import socket
import selectors
import signal
import logging
import argparse
from datetime import datetime

# configure logger output format
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('Load Balancer')


# used to stop the infinity loop
done = False

sel = selectors.DefaultSelector()

policy = None
mapper = None
cache = {}


# implements a graceful shutdown
def graceful_shutdown(signalNumber, frame):  
    logger.debug('Graceful Shutdown...')
    global done
    done = True


# n to 1 policy
class N2One:
    def __init__(self, servers):
        self.servers = servers  

    def select_server(self):
        return self.servers[0]

    def update(self, *arg):
        pass


# round robin policy
class RoundRobin:
    def __init__(self, servers):
        self.servers = servers
        self.pointer = 0

    def select_server(self):
        server = self.servers[self.pointer]
        self.pointer = (self.pointer + 1) % len(self.servers)
        return server
    
    def update(self, *arg):
        pass


# least connections policy
class LeastConnections:
    def __init__(self, servers):
        self.servers = servers
        self.clients_by_server = dict()
        for server in self.servers:
            self.clients_by_server[server] = 0

    def select_server(self):
        _server = self.servers[0]
        _min = self.clients_by_server[_server]
        for server in self.clients_by_server:
            count = self.clients_by_server[server]
            if count < _min:
                _min = count
                _server = server

        self.clients_by_server[_server] += 1
        return _server

    def update(self, *arg):
        if len(arg) > 0:
            server = arg[0]
            self.clients_by_server[server] -= 1


# least response time
class LeastResponseTime:
    def __init__(self, servers):
        self.servers = servers
        self.avg_by_server = dict()
        self.start_by_server = dict()
        for server in self.servers:
            self.avg_by_server[server] = None
            self.start_by_server[server] = None
        self.rr_pointer = 0

    def has_avg(self):
        for server in self.avg_by_server:
            if self.avg_by_server[server] is not None:
                return True
        return False

    def average(self, avg: float, num: float):
        if avg is None:
            return num
        return (avg+num)/2

    def select_server(self):
        start = datetime.now().timestamp()
        server = None

        if self.has_avg():
            for _server in self.avg_by_server:
                if self.avg_by_server[_server] is None:
                    server = _server
                    break
            if server is None:
                _min = None
                for _server in self.avg_by_server:
                    avg = self.avg_by_server[_server]
                    if _min is None or avg < _min:
                        _min = avg
                        server = _server
        else:
            server = self.servers[self.rr_pointer]
            self.rr_pointer = (self.rr_pointer + 1) % len(self.servers)

        print(f"select server {server}")
        self.start_by_server[server] = start
        return server

    def update(self, *arg):
        end = datetime.now().timestamp()
        if len(arg) > 0:
            server = arg[0]
            print(f"update server {server}")
            avg = self.avg_by_server[server]
            start = self.start_by_server[server]
            time = end - start
            self.avg_by_server[server] = self.average(avg, time)
            print(f"avgs: {self.avg_by_server}")


POLICIES = {
    "N2One": N2One,
    "RoundRobin": RoundRobin,
    "LeastConnections": LeastConnections,
    "LeastResponseTime": LeastResponseTime
}

class SocketMapper:
    def __init__(self, policy):
        self.policy = policy
        self.map = {}
        self.sock_server = {}

    def add(self, client_sock, upstream_server):
        logger.debug(f"client sock - {client_sock.getsockname()} {client_sock.getpeername()}")
        client_sock.setblocking(False)
        sel.register(client_sock, selectors.EVENT_READ, read)
        upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        upstream_sock.connect(upstream_server)
        logger.debug(f"Server sock - {upstream_sock.getsockname()} {upstream_sock.getpeername()}")
        upstream_sock.setblocking(False)
        sel.register(upstream_sock, selectors.EVENT_READ, read)
        logger.debug("Proxying to %s %s", *upstream_server)
        self.map[client_sock] = upstream_sock
        self.sock_server[upstream_sock] = upstream_server

    def delete(self, sock):
        sel.unregister(sock)
        sock.close()
        if sock in self.map:
            server_sock = self.map.pop(sock)
            server = self.sock_server.pop(server_sock)
            self.policy.update(server)

    def get_sock(self, sock):
        for client, upstream in self.map.items():
            if upstream == sock:
                return client
            if client == sock:
                return upstream
        return None
    
    def get_upstream_sock(self, sock):
        return self.map.get(sock)

    def get_all_socks(self):
        """ Flatten all sockets into a list"""
        return list(sum(self.map.items(), ()))

def accept(sock, mask):
    client, addr = sock.accept()
    logger.debug("Accepted connection %s %s", *addr)
    mapper.add(client, policy.select_server())


def read(conn,mask):
    logger.debug(f"read - sock  {conn.getsockname()} {conn.getpeername()}")
    data = conn.recv(4096)
    ts = datetime.now().timestamp()
    # No messages in socket, we can close down the socket
    if len(data) == 0:
        logger.debug(f"DELETE sock - {conn.getsockname()} {conn.getpeername()}")
        mapper.delete(conn)
    else:
        tokens = data.decode().strip().split()
        if tokens[0] == "GET":
            logger.debug("It is a request!")
            precision = tokens[1].strip("/")
            if precision in cache:
                logger.debug("response from cache")
                data = cache[precision][0]
                conn.send(data)
                return

        elif tokens[0] == "HTTP/1.0":
            logger.debug("It is a response!")
            precision = None
            for i in range(len(tokens)):
                token = tokens[i]
                if token == "Computing":
                    precision = tokens[i + 4]
                    break
            if len(cache) >= 5:
                oldest = None
                precision_remove = None
                for key in cache:
                    if oldest is None or cache[key][1] < oldest:
                        oldest = cache[key][1]
                        precision_remove = key
                logger.debug(f"remove from cache precision {precision_remove}")
                cache.pop(precision_remove, "")
            logger.debug(f"add to cache at {datetime.fromtimestamp(ts)} precision {precision}")
            cache[precision] = (data, ts)

        other_sock = mapper.get_sock(conn)
        other_sock.send(data)



def main(addr, servers, policy_class):
    global policy
    global mapper
    global cache

    # register handler for interruption 
    # it stops the infinite loop gracefully
    signal.signal(signal.SIGINT, graceful_shutdown)

    policy = policy_class(servers)
    mapper = SocketMapper(policy)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(addr)
    sock.listen()
    sock.setblocking(False)

    sel.register(sock, selectors.EVENT_READ, accept)

    try:
        logger.debug("Listening on %s %s", *addr)
        while not done:
            events = sel.select(timeout=1)
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
                
    except Exception as err:
        logger.error(err)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pi HTTP server')
    parser.add_argument('-a', dest='policy', choices=POLICIES)
    parser.add_argument('-p', dest='port', type=int, help='load balancer port', default=8080)
    parser.add_argument('-s', dest='servers', nargs='+', type=int, help='list of servers ports')
    args = parser.parse_args()
    
    servers = [('localhost', p) for p in args.servers]
    
    main(('127.0.0.1', args.port), servers, POLICIES[args.policy])
