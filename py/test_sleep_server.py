import os
import subprocess
import xmlrpc
from threading import Thread

from test_init import server_cnt, port

if __name__ == "__main__":
    # server_id = int(input("sleep server id: #"))

    ths = []
    for server_id in range(server_cnt):
        host_port = f"http://localhost:{port+server_id}"

        th = Thread(
            target=lambda hp: xmlrpc.client.ServerProxy(host_port).sleep(10),
            args=(host_port, )
        )
        th.start()
        ths.append(th)

    for th in ths:
        th.join()
