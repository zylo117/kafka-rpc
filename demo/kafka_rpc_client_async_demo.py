# Copyright (c) 2017-2020, Carl Cheung
# All rights reserved.

"""
Async Kafka RPC Client

Simulate a situation when multiple threads of Kafka RPC Client send request to Kafka RPC Server
"""

USE_GEVENT = True

if USE_GEVENT:
    import gevent
    as_completed = gevent.wait
    from gevent.threadpool import ThreadPoolExecutor as ThreadPoolExecutor
else:
    from concurrent.futures import ThreadPoolExecutor, as_completed

from kafka_rpc import KRPCClient

pool = ThreadPoolExecutor(128)

# assuming you kafka broker is on localhost:9092
krc = KRPCClient('localhost:9092', topic_name='sum')

# call method concurrently from client to server
# use pool.map if you like
futures = []
for i in range(128):
    futures.append(pool.submit(krc.add, 1, 2, timeout=20))

for future in as_completed(futures):
    result = future.result()
    print(result)

krc.close()
