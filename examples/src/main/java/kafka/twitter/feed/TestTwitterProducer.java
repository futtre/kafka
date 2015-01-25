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


/**
 * @author flutra
 * 
 * Example:
 * 1. Retrieve the latest Twitter messages based on hashtag filters.
 * 2. Using TwitterProducer, publish such messages to Kafka cluster 
 * on topics which equal hashtag filters.
 */
public class TestTwitterProducer {
	
	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092,localhost:9093");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
	
		TwitterProducer twitterProducer = new TwitterProducer(props);
		
		try {
			FilteredTwitterStream twitterStream = new FilteredTwitterStream(twitterProducer);
			twitterStream.run(args[0], args[1], args[2], args[3]);
		} catch (InterruptedException e) {
			System.out.println(e);
		}
	}
}