package kafka.twitter.feed;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

/**
 * @author flutra
 *
 * A generic example: retrieve tweets from Twitter Streaming API.
 * Populate a queue that is user-defined. This example contains 1000 tweets.
 */
public class TwitterStream {

	private BlockingQueue<String> _queue; 
	private StatusesSampleEndpoint _endpoint;
	private Authentication _auth;
	private List<String> _allTweets;
	
	public TwitterStream() {
		
		_allTweets = new ArrayList<String>();
		_queue = new LinkedBlockingQueue<String>(1000);
		_endpoint = new StatusesSampleEndpoint();
	}

	public void run(String consumerKey, String consumerSecret, String token, String tokenSecret) throws InterruptedException {
				
		_endpoint.stallWarnings(false);
		_auth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);
				
		// Create a new BasicClient. By default gzip is enabled.
		BasicClient client = new ClientBuilder()
			.name("sampleExampleClient")
			.hosts(Constants.STREAM_HOST)
            .endpoint(_endpoint)
            .authentication(_auth)
            .processor(new StringDelimitedProcessor(_queue))
            .build();

		// Establish a connection to Twitter API
		client.connect();

		// Process messages
		for (int msgRead = 0; msgRead < 1000; msgRead++) {
			if (client.isDone()) {
				System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
				break;
			}

			String msg = _queue.poll(5, TimeUnit.SECONDS);
			if (msg == null) {
				System.out.println("Did not receive a message in 5 seconds");
			} else {
				System.out.println(msg);
				_allTweets.add(msg);
	       	}
		}

		client.stop();

		// Print some statistics
		System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
	}
  
	public List<String> tweets() {
	  
		if (_allTweets.isEmpty())
			return null;
		else
			return _allTweets;
	}
}
