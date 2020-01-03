# Copyright (c) 2017-2020, Carl Cheung
# All rights reserved.

from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigSource
from confluent_kafka import KafkaException


class KafkaControl:
    def __init__(self, broker_url):
        """
        Args:
            broker_url:
        """
        self.a = AdminClient({'bootstrap.servers': broker_url})

    def create_topics(self, *topic_name, num_partitions=1, replication_factor=1):
        """Create topics

        Args:
            *topic_name:
            num_partitions:
            replication_factor:
        """

        new_topics = [NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor) for topic in
                      topic_name]
        # Call create_topics to asynchronously create topics, a dict
        # of <topic,future> is returned.
        fs = self.a.create_topics(new_topics)

        # Wait for operation to finish.
        # Timeouts are preferably controlled by passing request_timeout=15.0
        # to the create_topics() call.
        # All futures will finish at the same time.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} created".format(topic))
            except Exception as e:
                print("Failed to create topic {}: {}".format(topic, e))

    def delete_topics(self, *topic_name):
        """delete topics

        Args:
            *topic_name:
        """

        # Call delete_topics to asynchronously delete topics, a future is returned.
        # By default this operation on the broker returns immediately while
        # topics are deleted in the background. But here we give it some time (30s)
        # to propagate in the cluster before returning.
        #
        # Returns a dict of <topic,future>.
        fs = self.a.delete_topics(list(topic_name), operation_timeout=30)

        # Wait for operation to finish.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} deleted".format(topic))
            except Exception as e:
                print("Failed to delete topic {}: {}".format(topic, e))

    def resize_partitions(self, **kwargs):
        """resize partitions. Noted, kafka currently supports only increasing
        the number of partitions, but not decreasing. :param kwargs: examples:
        topic_name_1=4, topic_name_2=3, ...

        Args:
            **kwargs:
        """

        new_parts = [NewPartitions(topic, int(kwargs[topic])) for topic in kwargs]

        # Try switching validate_only to True to only validate the operation
        # on the broker but not actually perform it.
        fs = self.a.create_partitions(new_parts, validate_only=False)

        # Wait for operation to finish.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Additional partitions created for topic {}".format(topic))
            except Exception as e:
                print("Failed to add partitions to topic {}: {}".format(topic, e))

    def list_stat(self, what='all'):
        """list topics and cluster metadata :param what: all|topics|brokers
        :return:

        Args:
            what:
        """

        md = self.a.list_topics(timeout=10)

        print("Cluster {} metadata (response from broker {}):".format(md.cluster_id, md.orig_broker_name))

        if what in ("all", "brokers"):
            print(" {} brokers:".format(len(md.brokers)))
            for b in iter(md.brokers.values()):
                if b.id == md.controller_id:
                    print("  {}  (controller)".format(b))
                else:
                    print("  {}".format(b))

        if what not in ("all", "topics"):
            return

        print(" {} topics:".format(len(md.topics)))
        for t in iter(md.topics.values()):
            if t.error is not None:
                errstr = ": {}".format(t.error)
            else:
                errstr = ""

            print("  \"{}\" with {} partition(s){}".format(t, len(t.partitions), errstr))

            for p in iter(t.partitions.values()):
                if p.error is not None:
                    errstr = ": {}".format(p.error)
                else:
                    errstr = ""

                print("    partition {} leader: {}, replicas: {}, isrs: {}".format(
                    p.id, p.leader, p.replicas, p.isrs, errstr))


if __name__ == '__main__':
    kc = KafkaControl('localhost:9092')
    # kc.create_topics('mytopic', num_partitions=10, replication_factor=1)
    # kc.resize_partitions(mytopic=12)
    # kc.delete_topics('aiokafkarpc_out')
    kc.delete_topics('__consumer_offsets')
    # kc.delete_topics('numtest')
    kc.delete_topics('krpc_sum_client')
    kc.delete_topics('krpc_sum_server')
    kc.list_stat('all')
