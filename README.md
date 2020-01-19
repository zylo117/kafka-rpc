# Kafka RPC

---

## Introduction

Kafka RPC, a RPC protocol that based on [kafka](https://kafka.apache.org/), is meant to provide a swift, stable, reliable remote calling service.

The reason we love about kafka is its fault tolerance, scalability and wicked large throughput.

So if you want a RPC service with kafka features, kRPC is the kind of tool you're looking for.

---

### Installation

        pip install kafka-rpc

---

### FAQ

1. What is [RPC](https://en.wikipedia.org/wiki/Remote_procedure_call)?

   RPC is a request–response protocol. An RPC is initiated by the client, which sends a request message to a known remote server to execute a specified procedure with supplied parameters.

   The one that posts the job request is called Client, while the other one that gets the job and responses is called Server.

2. When should I use a RPC?

   When you have to call a function that doesn't belong to local process or computer, but you don't want to build up another complex network framework just to implement restful api or soap, etc.
   RPC is much faster than restful api and easier to use. This is only a very few lines of code to be adjusted to implement a RPC service.

3. Why [kafka-rpc](https://github.com/zylo117/kafka-rpc/), not the other RPC protocols like [zerorpc](https://github.com/0rpc/zerorpc-python), [grpc](https://github.com/grpc/grpc), [mprpc](https://github.com/studio-ousia/mprpc)?

   Here is the comparison.

   | RPC                                               | MiddleWare | Serialization | Speed(QPS) |                                     Features                                     |
   | ------------------------------------------------- | :--------: | :-----------: | :--------: | :------------------------------------------------------------------------------: |
   | [kafka-rpc](https://github.com/zylo117/kafka-rpc/)          |   kafka    |    msgpack    |    200+ (sync) 4700+(async)     | dynamic load rebalance, large throughput, data persistence, faster serialization |
   | [zerorpc](https://github.com/0rpc/zerorpc-python) |   zeromq   |    msgpack    |    450+    |  dynamic load rebalance(failed when all server are busy), faster serialization   |
   | [grpc](https://github.com/grpc/grpc)              |  unknown   |   protobuf    | not tested |       dynamic load rebalance, large throughput, support only function rpc        |
   | [mprpc](https://github.com/studio-ousia/mprpc)    |     no     |    msgpack    |   19000+   |                                    lightspeed                                    |

    Benchmark enviroment:

    Ubuntu 19.10 x64 (5.3.0-21-generic)

    Intel i5-8400

    ---

   The only reason that I developed kafka-rpc is that zerorpc failed me!

   After months of searching and testing, zerorpc was the best rpc service I'd ever used, but it's bugged!
   Normally, developers won't notice, because most of the time, we use RPC to post the job from one client directly to one server.

   But when you develop a distributing system, you will have to post K jobs of different types from N clients to M servers.

   The problem is that you don't really know which server is currently available, or right one for the job.

   And that's where load balancing comes in.

   Zeromq supports that, and zerorpc supports that too. In reverse proxy mode, zerorpc will post the jobs, but not sending to servers. Available servers will come looking for job actively. So the job will always be coped by the most available, therefore the most performant servers. However, sadly, zerorpc doesn't really queue up the job, so when you have no server available at all, the jobs will not wait in line but instead, be abandoned.

   That's why I need kafka. Unlike most of the MQ, kafka provides data persistence, so no more job abandon. When all servers is unavailable, the job will queue up and wait in line.

   If the client crash, jobs will still be on disk (kafka features), unharmed, with replicas(can you imagine that).

   Also, kafka-rpc supports dynamic scalability, servers can be always added to the cluster or be removed, so jobs will be fairly distribute to all the servers, and will be reroute to another healthy server if assigned server is down.

   Despite the minor disadvantages, they are all good tool developed by great programmers, you're always welcome to contribute to their original repositories and my forked [zerorpc](https://github.com/zylo117/zerorpc-python) and [mprpc](https://github.com/zylo117/mprpc), which support numpy array.

#### Next Step

- [X] optimize the QPS by allow asynchronous calls considering its large throughput advantage
- [X] use gevent instead of built-in threading to speed up and now it's 40% faster.
- [ ] rewrite it in cython
  
## Usage

### Assuming you already have a object and everything works, like this

#### local_call.py

    # Part1: define a class
    class Sum:
        def add(self, x, y):
            return x + y

    # Part2: instantiate a class to an object
    s = Sum()

    # Part3: call a method of the object
    result = s.add(1, 2)  # result = 3

### Then you can use RPC to run Part1 and Part2 on process1, and call Part3, the method of process 1 from process2

#### kafka_rpc_server_demo.py

    from kafka_rpc import KRPCServer

    # Part1: define a class
    class Sum:
        def add(self, x, y):
            return x + y

    # Part2: instantiate a class to an object
    s = Sum()

    # assuming you kafka broker is on 0.0.0.0:9092
    krs = KRPCServer('0.0.0.0:9092', handle=s, topic_name='sum')
    krs.server_forever()

#### kafka_rpc_client_demo.py

    from kafka_rpc import KRPCClient

    # assuming you kafka broker is on 0.0.0.0:9092
    krc = KRPCClient('0.0.0.0:9092', topic_name='sum')
    
    # call method from client to server
    result = krc.add(1, 2)
    
    print(result)
    
    krc.close()

    # you can find the returned result in result['ret']
    # result = {
    #     'ret': 3,
    #     'tact_time': 0.007979869842529297,  # total process time
    #     'tact_time_server': 0.006567955017089844,  # process time on server side
    #     'server_id': '192.168.1.x'  # client ip
    # }

## Advanced Usage

1. enable server side concurrency. Respectively, multiple requests must be sent concurrently.
    
    #### kafka_rpc_server_concurrency_demo.py
        
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
        
        # assuming you kafka broker is on 0.0.0.0:9092
        krs = KRPCServer('0.0.0.0:9092', handle=s, topic_name='sum', concurrent=128)
        krs.server_forever()

    #### kafka_rpc_client_async_demo.py

        from concurrent.futures import ThreadPoolExecutor, as_completed
        from kafka_rpc import KRPCClient
        
        pool = ThreadPoolExecutor(128)
        
        # assuming you kafka broker is on 0.0.0.0:9092
        krc = KRPCClient('0.0.0.0:9092', topic_name='sum')
        
        # call method concurrently from client to server
        # use pool.map if you like
        futures = []
        for i in range(128):
            futures.append(pool.submit(krc.add, 1, 2, timeout=20))
        
        for future in as_completed(futures):
            result = future.result()
            print(result)
        
        krc.close()


2. enable redis to speed up caching and temporarily store input/output data, by adding use_redis=True to KRPCClient, or specify redis port, db and password. But redis doesn't support async operations, it will crash, it's only faster in sync mode.

        krc = KRPCClient('0.0.0.0:9092', topic_name='sum', use_redis=True, redis_port=6379, redis_db=0, redis_password='kafka_rpc.no.1')

3. enhance the communication security, by adding verify=True or encrypt='whatever_password+you/want' or both to both of the client and the server.But enabling verification and encryption will have a little impact on performance.

        # basic verification and encryption
        krs = KRPCServer('0.0.0.0:9092', handle=s, topic_name='sum', verify=True, encrypt='whatever_password+you/want')
        krc = KRPCClient('0.0.0.0:9092', 9092, topic_name='sum', verify=True, encrypt='whatever_password+you/want')
        
        # advanced verification and encryption with custom hash function, input: bytes, output: bytes
        krs = KRPCServer('0.0.0.0:9092', handle=s, topic_name='sum', verify=True, encrypt='whatever_password+you want', verification=lambda x: sha3_224(x).hexdigest().encode())
        krc = KRPCClient('0.0.0.0:9092', topic_name='sum', verify=True, encrypt='whatever_password+you/want', verification=lambda x: sha3_224(x).hexdigest().encode())

4. use zstd to compress & decompress data
    
        krs = KRPCServer('0.0.0.0:9092', handle=s, topic_name='sum', use_compression=True)
        krc = KRPCClient('0.0.0.0:9092', 9092, topic_name='sum', use_compression=True)
        
### Warning

If use_redis=False, KRPCClient cannot be instantiated more than once

If use_redis=False, KRPCClient cannot be instantiated more than once

If use_redis=False, KRPCClient cannot be instantiated more than once

Do you remember now?
