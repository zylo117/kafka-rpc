# Copyright (c) 2017-2020, Carl Cheung
# All rights reserved.

from multiprocessing import Process

from krpc import KRPCClient
from krpc import KRPCServer


def start_server():
    class Sum:
        def add(self, x, y):
            return x + y

    krs = KRPCServer('localhost', 9092, Sum(), 'sum')
    krs.server_forever()


if __name__ == '__main__':
    p = Process(target=start_server)
    p.start()

    krc = KRPCClient('localhost', 9092, 'sum',
                     use_redis=False)
    z = krc.add(1, 2)
    print(z)

    for i in range(100):
        print(krc.add(1, 2))

    krc.close()

    p.terminate()
