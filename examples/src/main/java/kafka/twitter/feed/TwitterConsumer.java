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

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.message.MessageAndMetadata;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;


/**
 * @author flutra
 *
 * This is a high-level TwitterConsumer. It consumes Twitter messages on
 * all topics published by TwitterProducer in the cluster. 
 */
public class TwitterConsumer {
	
	private ConsumerConfig _consumerConfig;
	private ConsumerConnector _consumer;
	private ExecutorService _executor;
	private List<String> _topics;
	
	public TwitterConsumer(Properties consumerProps) {
		
		this._consumerConfig = new ConsumerConfig(consumerProps);
	}
	
	public TwitterConsumer(ConsumerConfig consumerConfig, List<String> topics) {
		
		this._consumerConfig = consumerConfig;
		this._topics = topics;
	}
	
	public void start() {
	
		System.out.println("Starting consumer...");		
		
		_executor = Executors.newCachedThreadPool();

		this._consumer = kafka.consumer.Consumer.createJavaConsumerConnector(_consumerConfig);
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		for (String topic : _topics) {
			topicCountMap.put(topic, 1);
		}
		
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = _consumer.createMessageStreams(topicCountMap);
		
		System.out.println("Adding streams for topics: ");
		for (String topic : _topics) {
			System.out.println(topic);
			final List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
			for (KafkaStream<byte[], byte[]> stream : streams) {
				_executor.submit(new TwitterConsumerTask(stream));
			}
		}				
	}

	public void stop() {
		
		System.out.println("Stopping consumer... ");
		
		if (_consumer != null)
			_consumer.commitOffsets();
	
		/*
		 * wait for the executor service to complete all tasks
		 * running in the pool of threads. The waiting time may be
		 * fine-tuned as one sees fit. For example, TwitterConsumer
		 * may be consuming only about 100 tweets or thousands of 
		 * tweets in several hundred topics.
		 */
		try {
			_executor.awaitTermination(120000, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	
		System.out.println("Shutting down consumer... ");
		
		if (_consumer != null)
			_consumer.shutdown();
		if (_executor != null)
			_executor.shutdown();	
	}
	
	static public class TwitterConsumerTask implements Runnable {
		
		private KafkaStream<byte[], byte[]> _stream;
		
		public TwitterConsumerTask(KafkaStream<byte[], byte[]> stream) {
			
			this._stream = stream;
		}
		
		@Override
		public void run() {
			
			System.out.println("Running the TwitterConsumerTask");
			
			ConsumerIterator<byte[], byte[]> it = _stream.iterator();
				
			while (it.hasNext()) {
				MessageAndMetadata<byte[], byte[]> msg = it.next();
				String topic = msg.topic();
				byte[] message = msg.message();
	            String messageString = new String(message, Charset.forName("UTF-8"));

				System.out.println("Topic: " + topic);
				System.out.println("Message: " + messageString);
				
			}

			System.out.println("TwitterConsumerTask done.");
		}	
	}
}
