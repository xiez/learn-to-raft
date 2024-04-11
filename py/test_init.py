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

if os.path.exists('app.log'):
    os.remove('app.log')

from server import Server

def start_server(server_id, host_port, peer_servers):
    s = Server(server_id, host_port, peer_servers)
    s.start()

def stop_server(server_id, host_port):
    s = Server(server_id, host_port, [])
    s.stop()


def log(msg):
    print(f"[{datetime.now()}] pid: {current_process().pid} {msg}")

server_cnt = 3
port = 8000

if __name__ == "__main__":
    servers = []
    for i in range(server_cnt):
        servers.append((i, f"localhost:{port+i}"))

    # start all servers
    log('starting servers ...')
    for sid, host_port in servers:
        peers = copy(servers)
        peers.remove((sid, host_port))
        pr = multiprocessing.Process(target=start_server, args=(sid, host_port, peers))
        pr.start()

    # main thread
    try:
        while 1:
            cluster_status = [-1] * server_cnt
            leader = None
     
            # ping servers & who is leader
            for i, host_port in servers:
                host_port = host_port if host_port.startswith('http') else 'http://' + host_port
                sp = xmlrpc.client.ServerProxy(host_port)
                try:
                    sp.ping()
                    cluster_status[i] = 0
                    if sp.is_leader():
                        leader = f"server#{i}"
                except:
                    cluster_status[i] = -1
     
            log(f'cluster status: {cluster_status}, leader is {leader}')
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, exiting.")
        sys.exit(0)


