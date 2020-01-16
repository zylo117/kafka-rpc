# Copyright (c) 2017-2020, Carl Cheung
# All rights reserved.

"""
Concurrent Kafka RPC Server

Enable Kafka RPC Server allowing multiple jobs running concurrently.
It comes in handy when you have multiple Kafka RPC Clients.
"""

import time

from kafka_rpc import KRPCServer


# Part1: define a class
class Sum:
    def add(self, x, y):

        # simulate blocking actions like I/O
        time.sleep(0.1)

        return x + y


# Part2: instantiate a class to an object
s = Sum()

# assuming you kafka broker is on localhost:9092
krs = KRPCServer('localhost:9092', handle=s, topic_name='sum', concurrent=128)
krs.server_forever()
