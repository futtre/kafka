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

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @author flutra
 *
 * TwitterProducer takes Twitter messages and publishes them
 * to the Kafka cluster. The topics to which these messages
 * are published are derived from Twitter hashtags. No partitioner 
 * is specified, therefore messages are published to the 
 * corresponding Kafka nodes using the default Kafka partitioner.
 */

public class TwitterProducer {

	private Producer<Integer, String> _producer;
	private ProducerConfig _producerConfig;
	
	public TwitterProducer(Properties configProps) {
	
		this._producerConfig = new ProducerConfig(configProps);
	}
	
	public TwitterProducer(ProducerConfig producerConfig) {
		
		this._producerConfig = producerConfig;
	}
	
	public void start() {
		
		_producer = new Producer<Integer, String>(_producerConfig);
	}
	
	public void stop() {
		
		_producer.close();
	}
	
	public void send(String topic, String tweetText) {

		final KeyedMessage<Integer, String> keyedMessage = new KeyedMessage<Integer, String>(topic, tweetText);
		_producer.send(keyedMessage);
	}

	//if the producer uses a user-specified partitioner
//	public void send(String topic, String key, byte[] tweetText) {
//		
//		final KeyedMessage<String, byte[]> keyedMessage = new KeyedMessage<>(topic, key, tweetText.getBytes());
//		_producer.send(keyedMessage);
//	}
	
}	