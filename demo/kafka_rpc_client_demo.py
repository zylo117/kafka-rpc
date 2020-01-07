# Copyright (c) 2017-2020, Carl Cheung
# All rights reserved.

"""
Basic Kafka RPC Client
"""

from kafka_rpc import KRPCClient

# assuming you kafka broker is on 0.0.0.0:9092
krc = KRPCClient('0.0.0.0', 9092, topic_name='sum')

# call method from client to server
result = krc.add(1, 2, timeout=20)

print(result)

krc.close()
