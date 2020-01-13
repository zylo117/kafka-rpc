# Copyright (c) 2017-2020, Carl Cheung
# All rights reserved.

"""
Basic Kafka RPC Client listening to multiple topics.
"""

from kafka_rpc import KRPCClient

# assuming you kafka broker is on 0.0.0.0:9092
krc = KRPCClient('localhost:9092', topic_name=[])

# call method from client to server
krc.subscribe('sum')
result1 = krc.add(1, 2, topic_name='sum', timeout=20)

krc.subscribe('diff')
result2 = krc.minus(1, 2, topic_name='diff', timeout=20)

print(result1)
print(result2)

krc.close()
