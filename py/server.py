from typing import List, Tuple
import sys
from datetime import datetime
import time
import random
import xmlrpc.server
from multiprocessing import current_process
from threading import Thread

from conf import logging
from raft import ConsensusModule, STATE_L

class MyServer:
    def __init__(self, cm):
        self.cm = cm

    def ping(self):
        return "pong"

    def is_leader(self):
        return self.cm.state == STATE_L

    def sleep(self, seconds):
        time.sleep(seconds)
        return seconds

    def request_vote(self, term, server_id):
        return self.cm.request_vote(term, server_id)

    def append_entries(self, term, leader_id):
        return self.cm.append_entries(term, leader_id)
    
class Server:
    def __init__(self, server_id, host_port, peer_servers: List[Tuple[int, str]]):
        self.server_id = server_id

        host, port = host_port.split(":")
        self.host = host
        self.port = int(port)

        self.peer_servers = peer_servers
        self.peer_clients = {}  # peer_id -> ServerProxy
        self.cm = None

    def log(self, func, msg):
        func(f"pid: {current_process().pid} server#{self.server_id} {msg}")

    def debug(self, msg):
        self.log(logging.debug, msg)

    def info(self, msg):
        self.log(logging.info, msg)

    def start(self):
        self.cm = ConsensusModule(self) # load CM
        self.serve()                    # start RPC server
        self.ping_peers()
        self.cm.run()           # start CM

        try:
            while 1:
                time.sleep(60)
        except KeyboardInterrupt:
            print("\nKeyboard interrupt received, exiting.")
            sys.exit(0)

    def stop(self):
        sys.exit(0)

    def serve(self):
        assert self.cm is not None
        def inner():
            server = xmlrpc.server.SimpleXMLRPCServer((self.host, self.port))
            server.register_instance(MyServer(cm=self.cm))
            self.info(f'serving RPC on localhost at port: {self.port} ...')
            server.serve_forever()

        th = Thread(target=inner)
        th.start()

    def ping_peers(self):
        assert len(self.peer_servers) > 0
        for sid, host_port in self.peer_servers:
            host_port = host_port if host_port.startswith('http') else 'http://' + host_port
            sp = xmlrpc.client.ServerProxy(host_port)
            while 1:
                try:
                    sp.ping()
                    self.info(f"connected to peer [{host_port}]")
                    self.peer_clients[sid] = sp
                    break
                except Exception as e:
                    time.sleep(1)

    def is_leader(self):
        return self.cm.state == STATE_L

    # def call(self, other_id, service_method, args):
    #     self.log(f"call peer [{other_id}] with {service_method}{args}")
    #     return getattr(self.peer_clients[other_id], service_method)(*args)
