# Copyright (c) 2017-2020, Carl Cheung
# All rights reserved.

"""
Basic Kafka RPC Server
"""

from kafka_rpc import KRPCServer


# Part1: define a class
class Sum:
    def add(self, x, y):
        return x + y


# Part2: instantiate a class to an object
s = Sum()

# assuming you kafka broker is on 0.0.0.0:9092
krs = KRPCServer('localhost:9092', handle=s, topic_name='sum', replication_factor=2, concurrent=2)
krs.server_forever()
