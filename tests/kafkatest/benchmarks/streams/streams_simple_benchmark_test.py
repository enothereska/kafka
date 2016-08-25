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

from ducktape.mark import ignore
from ducktape.mark import parametrize
from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.performance.streams_performance import StreamsSimpleBenchmarkService
import time

TOPIC = "simpleBenchmarkSourceTopic"
class StreamsSimpleBenchmarkTest(KafkaTest):
    """
    Simple benchmark of Kafka Streams.
    """

    def __init__(self, test_context):
        self.topics = {
            TOPIC: {'partitions': 9, 'replication-factor': 1}
        }
        super(StreamsSimpleBenchmarkTest, self).__init__(test_context, num_zk=1, num_brokers=3,
                                                         topics=self.topics)

        
    @parametrize(num_nodes=1)
    @parametrize(num_nodes=2)
    @parametrize(num_nodes=3)
    def test_simple_benchmark(self, num_nodes=1):
        """
        Run simple Kafka Streams benchmark
        """
        self.driver = StreamsSimpleBenchmarkService(self.test_context, num_nodes, self.kafka, 1000000L)
        self.driver.start()
        self.driver.wait()
        self.driver.stop()
        node = self.driver.node
        node.account.ssh("grep Performance %s" % self.driver.STDOUT_FILE, allow_fail=False)

        return self.driver.collect_data(node)
