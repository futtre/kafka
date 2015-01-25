/**
 * Copyright 2014 Flutra Osmani
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.twitter.feed;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;

/**
 * @author flutra
 *
 * Example:
 * Consume messages from the topics specified below.
 */
public class TestTwitterConsumer {

	public static void main(String[] args) {
		
		//example topics that were published to Kafka cluster
		List<String> topics = new ArrayList<String>();
		topics.add("hbc");
		topics.add("hosebird");
		topics.add("kafka");
		
		Properties props = new Properties();
		props.put("zookeeper.connect", "localhost:2181");
		props.put("group.id", "kafka-twitter-feed");
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		
		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		
		//consume messages from the specified topics
		TwitterConsumer twitterConsumer = new TwitterConsumer(consumerConfig, topics);
		twitterConsumer.start();		
		twitterConsumer.stop();
	}
}
