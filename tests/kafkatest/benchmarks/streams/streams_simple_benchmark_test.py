# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from pprint import pprint
from ducktape.mark import ignore
from ducktape.mark import parametrize
from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.performance.streams_performance import StreamsSimpleBenchmarkService
import time

TOPIC = "simpleBenchmarkSourceTopic"
TOPIC_KSTREAMKTABLE1="joinSourceTopic1kStreamKTable"
TOPIC_KSTREAMKTABLE2="joinSourceTopic2kStreamKTable"
TOPIC_KSTREAMKSTREAM1="joinSourceTopic1kStreamKStream"
TOPIC_KSTREAMKSTREAM2="joinSourceTopic2kStreamKStream"
TOPIC_KTABLEKTABLE1="joinSourceTopic1kTableKTable"
TOPIC_KTABLEKTABLE2="joinSourceTopic2kTableKTable"
class StreamsSimpleBenchmarkTest(KafkaTest):
    """
    Simple benchmark of Kafka Streams.
    """

    def __init__(self, test_context):
        self.topics = {
            TOPIC: {'partitions': 1, 'replication-factor': 1},
	    TOPIC_KSTREAMKTABLE1: {'partitions': 3, 'replication-factor': 1},
	    TOPIC_KSTREAMKTABLE2: {'partitions': 3, 'replication-factor': 1},
	    TOPIC_KSTREAMKSTREAM1: {'partitions': 3, 'replication-factor': 1},
	    TOPIC_KSTREAMKSTREAM2: {'partitions': 3, 'replication-factor': 1},
	    TOPIC_KTABLEKTABLE1: {'partitions': 3, 'replication-factor': 1},
	    TOPIC_KTABLEKTABLE2: {'partitions': 3, 'replication-factor': 1}
        }
        super(StreamsSimpleBenchmarkTest, self).__init__(test_context, num_zk=1, num_brokers=3,
                                                         topics=self.topics)

        
    @parametrize(num_nodes=1, specific_test='all')
    @parametrize(num_nodes=1, specific_test='kStreamKStreamJoin')
    @parametrize(num_nodes=3, specific_test='kStreamKStreamJoin')
    @parametrize(num_nodes=1, specific_test='kStreamKTableJoin')
    @parametrize(num_nodes=3, specific_test='kStreamKTableJoin')
    @parametrize(num_nodes=1, specific_test='kTableKTableJoin')
    @parametrize(num_nodes=3, specific_test='kTableKTableJoin')
    def test_simple_benchmark(self, num_nodes=1, specific_test='all'):
        numrecs = 10000L
        waitnumrecs = 1000L
        """
        Run simple Kafka Streams benchmark
        """
        self.driver = StreamsSimpleBenchmarkService(self.test_context, num_nodes, self.kafka, numrecs, specific_test, waitnumrecs)
        if (specific_test != 'all'):
            print "Preparing topic first..."
            self.driver.prepare_topic()

        print "Running test..."
        self.driver.start()
        self.driver.wait()
        self.driver.stop()
	data = self.driver.collect_data()
	pprint(data)
	return data
