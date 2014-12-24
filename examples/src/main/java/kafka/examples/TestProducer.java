///**
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// * 
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package kafka.examples;
//
//import java.util.*;
//
//import kafka.javaapi.producer.Producer;
//import kafka.producer.KeyedMessage;
//import kafka.producer.ProducerConfig;
//
//public class TestProducer {
//
//	public static void main(String[] args) {
//		
//		int events = 10;
//		Random rnd = new Random();		
//		Properties props = new Properties();
//		props.put("metadata.broker.list", "localhost:9092,localhost:9093");
//		props.put("serializer.class", "kafka.serializer.StringEncoder");
//		props.put("partitioner.class", "kafka.examples.TestPartitioner");
//		props.put("request.required.acks", "1");
//		props.put("metadata.fetch.timeout.ms", 100000);
//		ProducerConfig config = new ProducerConfig(props);
//		Producer<String, String> producer = new Producer<String, String>(config);
//	
////		for (String hashtag : myHashtags) {
////			for (String tweetStr : myTweets) {
////				JSONObject json;
////				try {
////					json = new JSONObject(tweetStr);			
////					if (json != null) {					
////						if (hashtag.equals(json.getString("name"))) {
////							List<String> tweetsPerTag = tagTweets.get(hashtag);
////							tweetsPerTag.add(tweetStr);
////							tagTweets.put(hashtag, tweetsPerTag);
////						}
////					}
////				} catch (JSONException e) {
////					json = null;
////					e.printStackTrace();
////				}					
////			}	
////		}
//		
//		for (int nEvents = 0; nEvents < events; nEvents++) { 
//			
//            long runtime = new Date().getTime();  
//            String key = "" + rnd.nextInt(255);
//            String message = runtime + "www.twitter.com" + key; 
//            KeyedMessage<String, String> data = new KeyedMessage<String, String>("TOT", key, message);
//            producer.send(data);
//     }
//		
//		//publish for each topic
////		for (String topic : myHashtags) {
////			for (String tweet : myTweets) {
////						
////				String key = "" + rnd.nextInt(255);
////				
////				KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key, tweet);
////				producer.send(data);
////			}
////		}
//		
//		producer.close();
//	}
//}

///////////////////////////////////////////////////////////////////

package kafka.examples;

import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class TestProducer {
	
	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092,localhost:9093");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		ProducerConfig producerConfig = new ProducerConfig(props);
		Producer<Integer, String> producer = new Producer<Integer, String>(producerConfig);
		

		int events = 50;
		for (int i = 0; i < events; i++) { 
            long runtime = new Date().getTime();  
            String message = runtime + "www.twitter.com"; 
            KeyedMessage<Integer, String> data = new KeyedMessage<Integer, String>(message, message);
            producer.send(data);
     }
		producer.close();
	}
}



