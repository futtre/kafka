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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

/**
 * @author flutra
 *
 *Example:
 * Retrieve tweets from the Twitter Streaming API based on hashtag filters.
 * (Other filters may be used to retrieve tweets)
 */
public class FilteredTwitterStream {
	
	private BlockingQueue<String> _queue; 
	private StatusesFilterEndpoint _endpoint;
	private Authentication _auth;
	private List<String> _topicTags;
	private TwitterProducer _twitterProducer;
	
	public FilteredTwitterStream(TwitterProducer twitterProducer) {
		
		_twitterProducer = twitterProducer;
		_queue = new LinkedBlockingQueue<String>(9000); //this can be configured to hold > or < messages
		_endpoint = new StatusesFilterEndpoint(); // the endpoint to connect to Twitter Streaming API
	}

	public void run(String consumerKey, String consumerSecret, String token, String tokenSecret) throws InterruptedException {
		
		//necessary application&user tokens to authenticate to Twitter API
		_auth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);
		
		//example hashtags we are looking for
		_topicTags = new ArrayList<String>();
		_topicTags.add("twitterapi");
		_topicTags.add("yolo");
		_endpoint.trackTerms(_topicTags);
		
		// Create a new BasicClient. By default gzip is enabled.
		BasicClient client = new ClientBuilder()
			.name("sampleExampleClient")
			.hosts(Constants.STREAM_HOST)
            .endpoint(_endpoint)
            .authentication(_auth)
            .processor(new StringDelimitedProcessor(_queue))
            .build();

		// Establish a connection (to Twitter API)
		client.connect();
		
		//Establish a connection (producer to Kafka cluster)
		_twitterProducer.start();
				
		// Process Twitter messages. Use producer to send them to the cluster
		// messageQueue is the number of messages retrieved from Twitter (can be re-configured)
		int messageQueue = 9000;
		while (messageQueue > 0) {
			String tweetString = _queue.poll(5, TimeUnit.SECONDS);
			TwitterMessage twitterMessage = new TwitterMessage(tweetString);
			List<String> hashtags = twitterMessage.getHashtags();
			
			for (String topic : hashtags) {
				if (_topicTags.contains(topic))
					_twitterProducer.send(topic, twitterMessage.getText());
			}
			messageQueue--;
		}
		
		client.stop();
		
		// Print some statistics
		System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
		
		_twitterProducer.stop();
	}
}