# Copyright (c) 2017-2020, Carl Cheung
# All rights reserved.

"""
Update Log:
1.0.1: init
1.0.2: change project name from krpc to kafka_rpc
1.0.3: add server side concurrency
1.0.4: allow increasing message_max_bytes
1.0.6: stop using a global packer or unpacker, to ensure thread safety.
1.0.8: use gevent instead of built-in threading to speed up about 40%
1.0.9: support message compression
"""

import datetime
import logging
import time
import traceback
import zlib
from concurrent.futures.thread import ThreadPoolExecutor

import socket
import pickle

from typing import Callable

import zstd

logger = logging.getLogger(__name__)

from confluent_kafka import Producer, Consumer, KafkaError
import msgpack
import msgpack_numpy

msgpack_numpy.patch()  # add numpy array support for msgpack

from kafka_rpc.aes import AESEncryption
from kafka_rpc.topic_manage import KafkaControl


class KRPCServer:
    def __init__(self, *addresses, handle, topic_name: str, server_name: str = None,
                 num_partitions: int = 64, replication_factor: int = 1,
                 max_polling_timeout: float = 0.001,
                 concurrent=False, **kwargs):
        """
        Init Kafka RPCServer.

        Multiple KRPCServer can be instantiated to balance to load.
        If any server is down, the other KRPCServer will automatically take it place.

        Args:
            addresses: kafka broker host, port, for examples: '192.168.1.117:9092'
            handle: any object
            topic_name: kafka topic_name, if topic exists,
                        the existing topic will be used,
                        create a new topic otherwise.
            server_name: krpc server name, if None, use ip instead.
            num_partitions: kafka topic num_partitions
            replication_factor: kafka topic replication_factor. Backup counts on other brokers. The larger replication_factor is, the slower but safer.
            max_polling_timeout: maximum time(seconds) to block waiting for message, event or callback.
            encrypt: default None, if not None, will encrypt the message with the given password. It will slow down performance.
            verify: default False, if True, will verify the message with the given sha3 checksum from the headers.
            ack: default False, if True, server will confirm the message status. Disable ack will double the speed, but not exactly safe.
            concurrent: default False, if False, the handle work in a local threads.
                        If concurrent is a integer K, KRPCServer will generate a pool of K threads,
                        so handle works in multiple threads.
                        Be aware that when benefiting from concurrency, KRPCClient should run in async mode as well.
                        If concurrency fails, the handle itself might not support multithreading.
            use_gevent: default True, if True, use gevent instead of asyncio. If gevent version is lower than 1.5, krpc will not run on windows.
            compression: default 'none', check https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md compression.codec. 'zstd' is bugged. Check https://github.com/confluentinc/confluent-kafka-python/issues/589
            use_compression: default False, custom compression using zstd.
        """
        bootstrap_servers = ','.join(addresses)
        kc = KafkaControl(bootstrap_servers)

        self.topic_name = topic_name

        self.server_topic = 'krpc_{}_server'.format(topic_name)
        self.client_topic = 'krpc_{}_client'.format(topic_name)

        # create a topic that receives requests from client
        kc.create_topics(self.server_topic, num_partitions=num_partitions,
                         replication_factor=replication_factor)

        # create a topic that send responses to client
        kc.create_topics(self.client_topic, num_partitions=num_partitions,
                         replication_factor=replication_factor)

        # set handle
        self.handle = handle

        # set server_name
        if server_name is None:
            self.server_name = get_ip()
        else:
            self.server_name = server_name + '_' + get_ip()

        # set max_polling_timeout
        assert max_polling_timeout > 0, 'max_polling_timeout must be greater than 0'
        self.max_polling_timeout = max_polling_timeout

        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'krpc',
            'auto.offset.reset': 'earliest',
            'auto.commit.interval.ms': 1000,
            'compression.codec': kwargs.get('compression', 'none')
        })

        # message_max_bytes = kwargs.get('message_max_bytes', 1048576),
        # queue_buffering_max_kbytes = kwargs.get('queue_buffering_max_kbytes', 1048576),
        # queue_buffering_max_messages = kwargs.get('queue_buffering_max_messages', 100000),
        try:
            message_max_bytes = kwargs['message_max_bytes']
        except KeyError:
            message_max_bytes = 1048576
        try:
            queue_buffering_max_kbytes = kwargs['queue_buffering_max_kbytes']
        except KeyError:
            queue_buffering_max_kbytes = 1048576
        try:
            queue_buffering_max_messages = kwargs['queue_buffering_max_messages']
        except KeyError:
            queue_buffering_max_messages = 100000

        if message_max_bytes > 1048576:
            logger.warning('message_max_bytes is greater than 1048576, '
                           'message.max.bytes and replica.fetch.max.bytes of '
                           'brokers\' config should be greater than this')

        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'on_delivery': self.delivery_report,

            # custom parameters
            'message.max.bytes': message_max_bytes,
            'queue.buffering.max.kbytes': queue_buffering_max_kbytes,
            'queue.buffering.max.messages': queue_buffering_max_messages,
            'compression.codec': kwargs.get('compression', 'none')
        })

        self.consumer.subscribe([self.server_topic])

        # custom callbacks, not implemented yet
        # self.callback_before_call = kwargs.get('callback_before_rpc', None)
        # self.callback_after_call = kwargs.get('callback_after_rpc', None)

        # set msgpack packer & unpacker, stop using a global packer or unpacker, to ensure thread safety.
        # self.packer = msgpack.Packer(use_bin_type=True)
        self.unpacker = msgpack.Unpacker(use_list=False, raw=False)

        # set status indicator
        self.is_available = True

        self.verify = kwargs.get('verify', False)
        self.verification_method = kwargs.get('verification', 'crc32')
        if self.verification_method == 'crc32':
            self.verification_method = lambda x: hex(zlib.crc32(x)).encode()
        elif isinstance(self.verification_method, Callable):
            self.verification_method = self.verification_method
        else:
            raise AssertionError('not supported verification function.')

        self.encrypt = kwargs.get('encrypt', None)
        if self.encrypt is not None:
            self.encrypt = AESEncryption(self.encrypt, encrypt_length=16)

        self.use_compression = kwargs.get('use_compression', False)

        self.is_closed = False

        # acknowledge, disable ack will double the speed, but not exactly safe.
        self.ack = kwargs.get('ack', False)

        # concurrency
        if isinstance(concurrent, int) and concurrent is not False:
            assert concurrent > 1, 'if enable concurrency, concurrent must be a integer greater than 1'
            use_gevent = kwargs.get('use_gevent', True)
            if use_gevent:
                from gevent.threadpool import ThreadPoolExecutor as gThreadPoolExecutor
                self.thread_pool = gThreadPoolExecutor(concurrent)
            else:
                self.thread_pool = ThreadPoolExecutor(concurrent)
        else:
            self.thread_pool = None

    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            logger.error('response failed: {}'.format(err))
        else:
            logger.info('response returned to {} [{}]'.format(msg.topic(), msg.partition()))

    @staticmethod
    def parse_request(msg_value):
        try:
            req = msgpack.unpackb(msg_value, use_list=False, raw=False)
        except Exception as e:
            logger.exception(e)
            # TODO: Exception should be responded immediately.

            req = None
        return req

    def server_forever(self):
        logger.info('start serving forever')
        while True:
            if self.is_closed:
                logger.info('user exit')
                self.close()
                break

            try:
                msg = self.consumer.poll(self.max_polling_timeout)

                if msg is None:
                    continue
                if msg.error():
                    logger.error('Request error: {}'.format(msg.error()))
                    continue

                task_id = msg.key()  # an uuid, the only id that pairs the request and the response

                if task_id == b'handshake':
                    self.producer.produce(self.client_topic, b'handshake', b'handshake',
                                          headers={
                                              'checksum': None
                                          })
                    self.producer.poll(0.0)
                    logger.info('handshake succeeded.')
                    continue

                if self.thread_pool is None:
                    self._local_call(msg, task_id)
                else:
                    self.thread_pool.submit(self._local_call, msg, task_id, )

            except (KeyboardInterrupt, SystemExit):
                self.is_closed = True

            except Exception as e:
                logger.exception(e)

    def _local_call(self, msg, task_id):
        value = msg.value()
        headers = msg.headers()
        timestamp = msg.timestamp()
        topic = msg.topic()
        logger.info('request received, topic: {} task id: {}'.format(topic, task_id))

        # get info from header, etc
        request_time = timestamp[1] / 1000
        received_time = time.time()

        checksum = headers[0][1]

        if self.verify:
            signature = self.verification_method(value)
            if checksum != signature:
                logger.error('checksum mismatch of task {}'.format(task_id))
                return

        if self.encrypt:
            value = self.encrypt.decrypt(value)

        if self.use_compression:
            value = zstd.decompress(value)

        req = self.parse_request(value)

        if req is None:
            return

        method_name = req['method_name']
        args = req['args']
        kwargs = req['kwargs']
        func = getattr(self.handle, method_name)

        # perform call
        self.is_available = False
        try:
            ret = func(*args, **kwargs)
            exception = None
            tb = None
        except Exception as e:
            ret = None
            exception = pickle.dumps(e, protocol=4)
            tb = traceback.format_exc()

        self.is_available = True

        tact_time = time.time() - received_time

        res = {
            'ret': ret,
            'tact_time': tact_time,
            'flight_time_request': received_time - request_time,
            'server_id': self.server_name,
            'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
            'exception': exception,
            'traceback': tb
        }

        # send return back to client
        res = msgpack.packb(res, use_bin_type=True)

        if self.use_compression:
            res = zstd.compress(res)

        if self.encrypt:
            res = self.encrypt.encrypt(res)

        if self.verify:
            checksum = self.verification_method(res)
        else:
            checksum = None

        self.producer.produce(self.client_topic, res, task_id,
                              headers={
                                  'checksum': checksum
                              })
        if self.ack:
            self.producer.poll(0.0)

    def close(self):
        self.consumer.close()
        self.producer.flush()


def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        ip = s.getsockname()[0]
    except:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip
