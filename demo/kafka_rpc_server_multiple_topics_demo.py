# Copyright (c) 2017-2020, Carl Cheung
# All rights reserved.

"""
Two Basic Kafka RPC Server
"""

import time
from multiprocessing import Process
from kafka_rpc import KRPCServer


def start_server1():
    # Part1: define a class
    class Sum:
        def add(self, x, y):
            return x + y

    # Part2: instantiate a class to an object
    s = Sum()

    # assuming you kafka broker is on 0.0.0.0:9092
    krs = KRPCServer('localhost:9092', handle=s, topic_name='sum')
    krs.server_forever()


def start_server2():
    # Part1: define a class
    class Diff:
        def minus(self, x, y):
            return x - y

    # Part2: instantiate a class to an object
    d = Diff()

    # assuming you kafka broker is on 0.0.0.0:9092
    krs = KRPCServer('localhost:9092', handle=d, topic_name='diff')
    krs.server_forever()


p1 = Process(target=start_server1, daemon=True)
p1.start()

p2 = Process(target=start_server2, daemon=True)
p2.start()

while True:
    time.sleep(1)
