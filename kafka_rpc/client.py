# Copyright (c) 2017-2020, Carl Cheung
# All rights reserved.

"""
Update Log:
1.0.1: init
1.0.2: change project name from krpc to kafka_rpc
1.0.4: allow increasing message_max_bytes
1.0.5: support subscribing to multiple topics
1.0.6: stop using a global packer or unpacker, to ensure thread safety.
1.0.8: use gevent instead of built-in threading to speed up about 40%
1.0.9: support message compression
1.0.10: add argument max_queue_len to control the length of QueueDict
1.0.11: change redis backend behavior
"""

import logging
import pickle
import time
import uuid
import zlib
from collections import deque
from typing import Callable, Union

import zstd

logger = logging.getLogger(__name__)

from confluent_kafka import Producer, Consumer, KafkaError
import msgpack
import msgpack_numpy

msgpack_numpy.patch()  # add numpy array support for msgpack

from kafka_rpc.aes import AESEncryption


class KRPCClient:
    def __init__(self, *addresses, topic_name: Union[str, list],
                 max_polling_timeout: float = 0.001, **kwargs):
        """
        Init Kafka RPCClient.

        Not like the most of the RPC protocols,
        Only one KRPCClient can run on a single Kafka topic.

        If you insist using multiple KRPCClient instances,
        redis must be used, pass argument use_redis=True.

        Args:
            addresses: kafka broker host, port, for examples: '192.168.1.117:9092'
            topic_name: kafka topic_name(s), if topic exists,
                        the existing topic will be used,
                        create a new topic otherwise.
            max_polling_timeout: maximum time(seconds) to block waiting for message, event or callback.
            encrypt: default None, if not None, will encrypt the message with the given password. It will slow down performance.
            verify: default False, if True, will verify the message with the given sha3 checksum from the headers.
            use_redis: default False, if True, use redis as cache, built-in QueueDict otherwise.
            ack: default False, if True, server will confirm the message status. Disable ack will double the speed, but not exactly safe.
            use_gevent: default True, if True, use gevent instead of asyncio. If gevent version is lower than 1.5, krpc will not run on windows.
            compression: default 'none', check https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md compression.codec. 'zstd' is bugged. Check https://github.com/confluentinc/confluent-kafka-python/issues/589
            use_compression: default False, custom compression using zstd.
            max_queue_len: int, default 1024, if use_redis is False, a QueueDict will cache results with the length of max_queue_len. This should be as low as it can be, otherwise OOM.
        """

        bootstrap_servers = ','.join(addresses)

        assert isinstance(topic_name, str) or isinstance(topic_name, list)
        self.topic_names = [topic_name] if isinstance(topic_name, str) else topic_name
        # self.server_topics = ['krpc_{}_server'.format(topic_name) for topic_name in self.topic_names]
        self.client_topics = ['krpc_{}_client'.format(topic_name) for topic_name in self.topic_names]

        # set max_polling_timeout
        assert max_polling_timeout > 0, 'max_polling_timeout must be greater than 0'
        self.max_polling_timeout = max_polling_timeout

        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'krpc',
            'auto.offset.reset': 'earliest',
            'auto.commit.interval.ms': 1000,
            'compression.codec': kwargs.get('compression_codec', 'none')
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
            'compression.codec': kwargs.get('compression_codec', 'none')
        })

        # add redis cache, for temporarily storage of returned data
        self.use_redis = kwargs.get('use_redis', False)
        self.expire_time = kwargs.get('expire_time', 600)
        if self.use_redis:
            import redis
            redis_host = kwargs.get('redis_host', 'localhost')
            redis_port = kwargs.get('redis_port', 6379)
            redis_db = kwargs.get('redis_db', 0)
            redis_password = kwargs.get('redis_password', None)
            self.cache = redis.Redis(redis_host, redis_port, redis_db, redis_password)
            self.cache_channel = self.cache.pubsub()
        else:
            self.cache = QueueDict(maxlen=kwargs.get('max_queue_len', 1024), expire=self.expire_time)

        # set msgpack packer & unpacker, stop using a global packer or unpacker, to ensure thread safety.
        # self.packer = msgpack.Packer(use_bin_type=True)
        # self.unpacker = msgpack.Unpacker(use_list=False, raw=False)

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
        # coroutine pool
        use_gevent = kwargs.get('use_gevent', True)
        if use_gevent:
            from gevent.threadpool import ThreadPoolExecutor as gThreadPoolExecutor
            self.pool = gThreadPoolExecutor(1)
        else:
            from aplex import ThreadAsyncPoolExecutor
            self.pool = ThreadAsyncPoolExecutor(pool_size=1)
        self.pool.submit(self.wait_forever)

        # handshake, if's ok not to handshake, but the first rpc would be slow.
        if kwargs.get('handshake', True):
            self.handshaked = {}
        self.subscribe(*self.topic_names)

        # acknowledge, disable ack will double the speed, but not exactly safe.
        self.ack = kwargs.get('ack', False)

    def subscribe(self, *topic_names):
        if not topic_names:
            return

        for topic_name in topic_names:
            client_topic = 'krpc_{}_client'.format(topic_name)

            self.topic_names.append(topic_name)
            self.client_topics.append(client_topic)

        self.consumer.subscribe(self.client_topics)
        logger.info('adding consumer subscription of: {}'.format(topic_names))

        if hasattr(self, 'handshaked'):
            for topic_name in topic_names:
                self.handshaked[topic_name] = False
                server_topic = 'krpc_{}_server'.format(topic_name)
                self.producer.produce(server_topic, b'handshake', b'handshake',
                                      headers={
                                          'checksum': None
                                      })
                self.producer.poll(0.0)
                logger.info('sending handshake to {}'.format(server_topic))
                for i in range(15):
                    if self.handshaked[topic_name]:
                        logger.info('handshake of {} succeeded.'.format(topic_name))
                        break
                    time.sleep(2)
                else:
                    logger.error('failed to handshake with {}'.format(server_topic))

    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            logger.error('request failed: {}'.format(err))
        else:
            logger.info('request sent to {} [{}]'.format(msg.topic(), msg.partition()))

    @staticmethod
    def parse_response(msg_value):
        try:
            res = msgpack.unpackb(msg_value, use_list=False, raw=False)
        except Exception as e:
            logger.exception(e)

            res = None
        return res

    def call(self, method_name, *args, **kwargs):
        # rpc call timeout
        # WARNING: if the rpc method has an argument named timeout, it will be not be passed.
        timeout = kwargs.pop('timeout', 10)

        # get topic_name
        topic_name = kwargs.pop('topic_name', self.topic_names[0])
        server_topic = 'krpc_{}_server'.format(topic_name)

        start_time = time.time()

        # send request back to server
        req = {
            'method_name': method_name,
            'args': args,
            'kwargs': kwargs
        }

        req = msgpack.packb(req, use_bin_type=True)

        if self.use_compression:
            req = zstd.compress(req)

        if self.encrypt:
            req = self.encrypt.encrypt(req)

        if self.verify:
            checksum = self.verification_method(req)
        else:
            checksum = None

        task_id = uuid.uuid4().hex

        self.producer.produce(server_topic, req, task_id,
                              headers={
                                  'checksum': checksum
                              })

        # waiting for response from server sync/async
        res, flight_time_response = self.poll_result_from_cache(task_id, timeout)

        if self.ack:
            self.producer.poll(0.0)

        # do something to the response
        ret = res['ret']
        tact_time_server = res['tact_time']
        flight_time_request = res['flight_time_request']
        server_id = res['server_id']
        exception = res['exception']
        tb = res['traceback']

        if exception is not None:
            exception = pickle.loads(exception)
            logger.exception(exception)
            if tb is not None:
                logger.error(tb)

        end_time = time.time()

        return {
            'ret': ret,
            'tact_time': end_time - start_time,
            'tact_time_server': tact_time_server,
            'server_id': server_id,
            'flight_time_request': flight_time_request,
            'flight_time_response': flight_time_response
        }

    def wait_forever(self):
        while True:
            if self.is_closed:
                logger.info('user exit')
                break

            try:
                msg = self.consumer.poll(self.max_polling_timeout)

                if msg is None:
                    continue
                if msg.error():
                    logger.error("consumer error: {}".format(msg.error()))
                    continue

                task_id = msg.key()  # an uuid, the only id that pairs the request and the response
                topic_name = msg.topic()

                if task_id == b'handshake':
                    try:
                        real_topic_name = '_'.join(topic_name.split('_')[1:-1])
                    except:
                        logger.error('invalid topic name {}'.format(topic_name))
                        continue
                    self.handshaked[real_topic_name] = True
                    continue

                res = msg.value()
                headers = msg.headers()
                timestamp = msg.timestamp()
                checksum = headers[0][1]

                if self.verify:
                    signature = self.verification_method(res)
                    if checksum != signature:
                        logger.error('checksum mismatch of task {}'.format(task_id))
                        continue

                if self.use_redis:
                    self.cache.hset(task_id, b'result', res)
                    self.cache.hset(task_id, b'flight_time_response', time.time() - timestamp[1] / 1000)
                    self.cache.expire(task_id, self.expire_time)
                else:
                    self.cache[task_id] = res, time.time() - timestamp[1] / 1000

                # send signal for polling to search for result
                ...

            except Exception as e:
                logger.exception(e)

    def poll_result_from_cache(self, task_id, timeout=10):
        """
        poll_result_from_cache after receiving a signal from waiting
        Args:
            task_id:
            timeout:

        Returns:

        """
        loop_times = int(timeout / self.max_polling_timeout)
        task_id = task_id.encode()
        if self.use_redis:
            for _ in range(loop_times):
                res_exists = self.cache.hexists(task_id, 'result')

                # if still no response yet, continue polling
                if not res_exists:
                    continue

                res = self.cache.hget(task_id, b'result')
                flight_time_response = self.cache.hget(task_id, b'flight_time_response')

                break
            else:
                raise TimeoutError
        else:
            for _ in range(loop_times):
                try:
                    res = self.cache[task_id]
                    flight_time_response = res[1]
                    res = res[0]
                    break
                except:
                    time.sleep(self.max_polling_timeout)
            else:
                raise TimeoutError

        if self.encrypt:
            res = self.encrypt.decrypt(res)

        if self.use_compression:
            res = zstd.decompress(res)

        res = self.parse_response(res)

        return res, flight_time_response

    def __getattr__(self, method_name):
        return lambda *args, **kwargs: self.call(method_name, *args, **kwargs)

    def close(self):
        self.is_closed = True
        if self.use_redis:
            self.cache_channel.close()
            self.cache.close()
        self.consumer.close()
        self.producer.flush()
        self.pool.shutdown()


class QueueDict:
    def __init__(self, maxlen=0, expire=128):
        assert isinstance(maxlen, int) and maxlen >= 0
        assert isinstance(expire, int) and expire >= 0

        self.queue_key = deque()
        self.queue_val = deque()
        self.queue_duration = deque()
        self.maxlen = maxlen
        self.expire = expire

    def __setitem__(self, key, val):
        self.queue_key.append(key)
        self.queue_val.append(val)
        self.queue_duration.append(time.time())

        self.remove_oldest()

    def __getitem__(self, key):
        idx = self.queue_key.index(key)
        val = self.queue_val[idx]
        self.queue_key.remove(self.queue_key[idx])
        self.queue_val.remove(self.queue_val[idx])
        self.queue_duration.remove(self.queue_duration[idx])
        return val

    def remove_oldest(self):
        if self.maxlen is not None:
            while True:
                if len(self.queue_key) > self.maxlen:
                    self.queue_key.popleft()
                    self.queue_val.popleft()
                    self.queue_duration.popleft()
                else:
                    break

        if self.expire is not None:
            num_pop = 0
            for i in range(len(self.queue_duration)):
                duration = time.time() - self.queue_duration[i - num_pop]
                if duration > self.expire:
                    self.queue_key.popleft()
                    self.queue_val.popleft()
                    self.queue_duration.popleft()

                    num_pop += 1
                else:
                    break
