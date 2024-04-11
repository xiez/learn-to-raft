import sys
import os
from copy import copy
from datetime import datetime
import time
from threading import Thread
import multiprocessing
from multiprocessing import current_process
from multiprocessing.pool import ThreadPool
import xmlrpc

from server import Server
from test_init import server_cnt, port

if __name__ == "__main__":
    server_id = int(input("start server id: #"))

    host_port = f"localhost:{port+server_id}"

    servers = []
    for i in range(server_cnt):
        servers.append((i, f"localhost:{port+i}"))

    peers = copy(servers)
    peers.remove((server_id, host_port))
        
    s = Server(server_id, host_port, peers)
    s.start()
