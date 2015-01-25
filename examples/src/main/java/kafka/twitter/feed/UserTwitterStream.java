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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.DirectMessage;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.User;
import twitter4j.UserList;
import twitter4j.UserStreamListener;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.UserstreamEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.twitter4j.Twitter4jUserstreamClient;

public class UserTwitterStream {

	private BlockingQueue<String> _queue;
	private UserstreamEndpoint _endpoint;
	private Authentication _auth;
	private ExecutorService _executor;
	private TwitterProducer _twitterProducer;
	
	public UserTwitterStream(TwitterProducer twitterProducer) {
	
		_queue = new LinkedBlockingQueue<String>(1000);
		_endpoint = new UserstreamEndpoint();	
		_twitterProducer = twitterProducer;
	}
	
	public void run(String consumerKey, String consumerSecret, String token, String tokenSecret) throws InterruptedException {
		
		// Start a twitterProducer
		_twitterProducer.start();
		
		_auth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);
		_endpoint.allReplies(true);
		
		// Create a new BasicClient. By default gzip is enabled.
		BasicClient client = new ClientBuilder()
			.name("userClientEx")
			.hosts(Constants.USERSTREAM_HOST)
		    .endpoint(_endpoint)
		    .authentication(_auth)
		    .processor(new StringDelimitedProcessor(_queue))
		    .build();
		
		_executor = Executors.newSingleThreadExecutor();
								
		//listeners
		UserStreamListener listener = new UserStreamListener() {

			@Override
			public void onStatus(Status status) {
				
				System.out.println("New tweet(s): " + status.getText());
				System.out.println("Written by " + status.getUser().getName() + " in " + status.getLang());
								
				HashtagEntity[] hashtags = status.getHashtagEntities();
				
				for (HashtagEntity hashtag : hashtags) {
					System.out.println("Producing to " + hashtag.getText() + "...");
					_twitterProducer.send(hashtag.getText(), status.getText());
				}
			}

			@Override
			public void onDeletionNotice(
					StatusDeletionNotice statusDeletionNotice) {
				
				System.out.println("Deleting tweet...");
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				
				System.out.println("Number of limited statuses: " + numberOfLimitedStatuses);
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				
				System.out.println("onScrubGeo event.");
				System.out.println("userId: " + userId);
				System.out.println("updtoStatusId: " + upToStatusId);
			}

			@Override
			public void onStallWarning(StallWarning warning) {
				
				System.out.println("Stall Warning: " + warning.getMessage());
			}

			@Override
			public void onException(Exception ex) {
				
				System.out.println("The following exception occurred: " + ex.getMessage());
				System.out.println("due to " + ex.getCause());
			}

			@Override
			public void onDeletionNotice(long directMessageId, long userId) {
				
				System.out.println("Deletion notice from " + userId);
			}

			@Override
			public void onFriendList(long[] friendIds) {
				
				System.out.println("My List of friends: ");
				for (long friend : friendIds) {
					System.out.print(friend + ", ");
				}
			}

			@Override
			public void onFavorite(User source, User target,
					Status favoritedStatus) {
				
				System.out.println("User " + source.getName() + " favorited a tweet from " + target.getName());
				System.out.println(favoritedStatus.getText());
			}

			@Override
			public void onUnfavorite(User source, User target,
					Status unfavoritedStatus) {
				
				System.out.println("User " + source.getName() + " unfavorited a tweet from " + target.getName());
				System.out.println(unfavoritedStatus.getText());
			}

			@Override
			public void onFollow(User source, User followedUser) {
				
				System.out.println("User: " + source.getName() + " followed " + followedUser.getName());
			}

			@Override
			public void onUnfollow(User source, User unfollowedUser) {
				
				System.out.println("User: " + source.getName() + " unfollowed " + unfollowedUser.getName());
			}

			@Override
			public void onDirectMessage(DirectMessage directMessage) {
				
				System.out.println("Received a direct message: " + directMessage.getText());
			}

			@Override
			public void onUserListMemberAddition(User addedMember,
					User listOwner, UserList list) {
				
				System.out.println("User " + addedMember.getName() + " was added to list " + list.getFullName() + " by " + listOwner.getName());
			}

			@Override
			public void onUserListMemberDeletion(User deletedMember,
					User listOwner, UserList list) {
				
				System.out.println("User " + deletedMember.getName() + " was deleted from list " + list.getFullName() + " by " + listOwner.getName());
			}

			@Override
			public void onUserListSubscription(User subscriber, User listOwner,
					UserList list) {
				
				System.out.println("User " + subscriber.getName() + " subscribed to " + list.getFullName());
			}

			@Override
			public void onUserListUnsubscription(User subscriber,
					User listOwner, UserList list) {
				
				System.out.println("User " + subscriber.getName() + " unsubscribed from " + list.getFullName());
			}

			@Override
			public void onUserListCreation(User listOwner, UserList list) {
				
				System.out.println("User " + listOwner.getName() + " created list " + list.getFullName());
			}

			@Override
			public void onUserListUpdate(User listOwner, UserList list) {
				
				System.out.println("User " + listOwner.getName() + " updated list " + list.getFullName());
			}

			@Override
			public void onUserListDeletion(User listOwner, UserList list) {
				
				System.out.println("User " + listOwner.getName() + " deleted list " + list.getFullName());
			}

			@Override
			public void onUserProfileUpdate(User updatedUser) {
				
				System.out.println("Updated user profile: " + updatedUser.toString());
			}

			@Override
			public void onBlock(User source, User blockedUser) {
				
				System.out.println("User " + source.getName() + " blocked user " + blockedUser.getName());
			}

			@Override
			public void onUnblock(User source, User unblockedUser) {
				
				System.out.println("User " + source.getName() + " unblocked user " + unblockedUser.getName());
			}
		};
		
		List<UserStreamListener> listeners = new ArrayList<UserStreamListener>();
		listeners.add(listener);
		
		Twitter4jUserstreamClient wrapperClient = new Twitter4jUserstreamClient(client, _queue, listeners, _executor);
		
		System.out.println("connecting...");
		
		wrapperClient.connect();
					
		System.out.println("processing...");
		
		wrapperClient.process();
		
		Thread.sleep(5000);
		
		System.out.println("ending...");
		client.stop();	
		System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
		
		_twitterProducer.stop();
		_executor.shutdown();		
	}
	
	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092,localhost:9093");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
	
		TwitterProducer twitterProducer = new TwitterProducer(props);
		
		try {
			UserTwitterStream twitterStream = new UserTwitterStream(twitterProducer);
			twitterStream.run(args[0], args[1], args[2], args[3]);
		} catch (InterruptedException e) {
			System.out.println(e);
		}
	}
}
