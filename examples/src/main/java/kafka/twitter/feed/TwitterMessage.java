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

import twitter4j.JSONArray;
import twitter4j.JSONException;
import twitter4j.JSONObject;

/**
 * @author flutra
 *
 * Twitter-message class.
 */
public class TwitterMessage {

	private JSONObject _tweetJsonObject;
	
	public TwitterMessage(String tweetText) {
		
		try {
			_tweetJsonObject = new JSONObject(tweetText);
		} catch (JSONException e) {
			e.printStackTrace();
		}			
	}
	
	public List<String> getHashtags() {
		
		List<String> resultHashtags = new ArrayList<String>();
		
		try {
			JSONArray hashtags = _tweetJsonObject.getJSONObject("entities").getJSONArray("hashtags");
			
			for (int i = 0; i < hashtags.length(); i++) {
				
				String[] hashtagElems = hashtags.getString(i).split(":")[1].split(",");
				String hashtag = hashtagElems[0].substring(1, hashtagElems[0].length() - 1);
				resultHashtags.add(hashtag);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		return resultHashtags;	
	}
	
	public String getText() {
		
		String resultText = new String();
		
		try {
			resultText = _tweetJsonObject.getString("text");
		} catch (JSONException e) {
			e.printStackTrace();
		}				
		
		return resultText;
	}
	
	public String getUser() {
		
		String username = new String();
		
		try {
			username = _tweetJsonObject.getJSONObject("user").getString("screen_name");
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return username;
	}
	
	public String getCreatedDate() {
		
		String dateStr = new String();
		
		try {
			dateStr = _tweetJsonObject.getString("created_at");
		} catch (JSONException e) {
			e.printStackTrace();
		}
	
		return dateStr; 
		
	}
	
	@Override
	public String toString() {
		
		StringBuilder str = new StringBuilder("TwitterMessage {");
		
		str.append(getCreatedDate());		
		str.append("\thashtag(s): ");
		for (String hashtag : getHashtags()) {
			str.append(hashtag).append(" ");			
		}
		str.append("\tuser: ").append(getUser());		
		str.append("\ttext: \"").append(getText()).append("\"");
			
		str.append("}");

		return str.toString();
	}
	
	//TODO
//	 @Override
//	    public byte[] getMessageBytes() {
//	        return message.getBytes(Charset.forName("UTF-8"));
//	    }

}