import sys
import os
from copy import copy
from datetime import datetime
import time
from threading import Thread
import multiprocessing
from multiprocessing import current_process
from multiprocessing.pool import ThreadPool
import xmlrpc.client

port = 8000

if __name__ == "__main__":

    leader_n = int(input("input leader server #"))
    server = f"http://localhost:{port+leader_n}"
    sp = xmlrpc.client.ServerProxy(server)

    for i in range(10):
        res = sp.submit(f"SET x {i}")

    print(f"submit result: {res}")


