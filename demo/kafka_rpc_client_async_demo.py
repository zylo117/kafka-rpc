# Copyright (c) 2017-2020, Carl Cheung
# All rights reserved.

"""
Async Kafka RPC Client

Simulate a situation when multiple threads of Kafka RPC Client send request to Kafka RPC Server
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka_rpc import KRPCClient

pool = ThreadPoolExecutor(128)

# assuming you kafka broker is on 0.0.0.0:9092
krc = KRPCClient('0.0.0.0', 9092, topic_name='sum')

# call method concurrently from client to server
# use pool.map if you like
futures = []
for i in range(128):
    futures.append(pool.submit(krc.add, 1, 2, timeout=20))

for future in as_completed(futures):
    result = future.result()
    print(result)

krc.close()
