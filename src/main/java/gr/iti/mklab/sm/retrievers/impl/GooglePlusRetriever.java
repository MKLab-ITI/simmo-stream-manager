package gr.iti.mklab.sm.retrievers.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.DateTime;
import com.google.api.services.plus.Plus;
import com.google.api.services.plus.Plus.People;
import com.google.api.services.plus.Plus.People.Get;
import com.google.api.services.plus.Plus.People.Search;
import com.google.api.services.plus.PlusRequestInitializer;
import com.google.api.services.plus.model.Activity;
import com.google.api.services.plus.model.ActivityFeed;
import com.google.api.services.plus.model.PeopleFeed;
import com.google.api.services.plus.model.Person;

import gr.iti.mklab.simmo.core.Item;
import gr.iti.mklab.simmo.core.UserAccount;
import gr.iti.mklab.simmo.core.documents.Post;
import gr.iti.mklab.simmo.core.items.Media;
import gr.iti.mklab.simmo.impl.posts.GooglePlusPost;
import gr.iti.mklab.simmo.impl.users.GooglePlusAccount;
import gr.iti.mklab.sm.Credentials;
import gr.iti.mklab.sm.feeds.AccountFeed;
import gr.iti.mklab.sm.feeds.GroupFeed;
import gr.iti.mklab.sm.feeds.KeywordsFeed;
import gr.iti.mklab.sm.retrievers.Response;
import gr.iti.mklab.sm.retrievers.SocialMediaRetriever;

/**
 * Class responsible for retrieving Google+ content based on keywords or google+ users
 * The retrieval process takes place through Google API
 * 
 * @author ailiakop
 */
public class GooglePlusRetriever extends SocialMediaRetriever {
	
	private Logger logger = Logger.getLogger(GooglePlusRetriever.class);
	
	private static final HttpTransport transport = new NetHttpTransport();
	private static final JsonFactory jsonFactory = new JacksonFactory();
	
	private Plus plusSrv;
	private String GooglePlusKey;

	public GooglePlusRetriever(Credentials credentials) throws Exception {
		super(credentials);
		
		if (credentials.getKey() == null) {
			logger.error("GooglePlus requires authentication.");
			throw new Exception("GooglePlus requires authentication.");
		}
		
		GooglePlusKey = credentials.getKey();
		GoogleCredential credential = new GoogleCredential();
		plusSrv = new Plus.Builder(transport, jsonFactory, credential)
						.setApplicationName("SocialSensor")
						.setHttpRequestInitializer(credential)
						.setPlusRequestInitializer(new PlusRequestInitializer(GooglePlusKey)).build();
	}

	@Override
	public Response retrieveAccountFeed(AccountFeed feed, Integer maxRequests) {
		
		Response response = new Response();
		List<Post> posts = new ArrayList<Post>();
		List<Media> media = new ArrayList<Media>();
		
		Date sinceDate = feed.getSinceDate();
		String label = feed.getLabel();
		
		int numberOfRequests = 0;
		
		String uName = feed.getUsername();
		String userID = feed.getId();

		if(uName == null && userID == null) {
			logger.info("#GooglePlus : No account feed");
			response = getResponse(posts, media, numberOfRequests);
			return response;
		}
				
		//Retrieve userID from Google+
		UserAccount streamUser = null;
		try {
			if(userID == null) {
				// userid is not available. search with username
				Search searchPeople = plusSrv.people().search(uName);
				searchPeople.setMaxResults(50L);
				
				PeopleFeed peopleFeed = searchPeople.execute();
				numberOfRequests++;

				List<Person> personsFound = peopleFeed.getItems();
				for(Person person : personsFound) {
					if(person.getUrl().equals("https://plus.google.com/+" + uName) || person.getDisplayName().equals(uName)) {
						userID = person.getId();
						streamUser = getStreamUser(userID);
						break;
					}
				}
			}
			else {
				numberOfRequests++;
				streamUser = getStreamUser(userID);
				uName = streamUser.getUsername();
			}
		} catch (Exception e) {
			logger.error(e);
			response = getResponse(posts, media, numberOfRequests);
			return response;
		}

		if(streamUser == null) {
			logger.error("User not found. Feed: (" + feed.getId() + "");
			response = getResponse(posts, media, numberOfRequests);
			return response;
		}
		
		boolean isFinished = false, sinceDateReached = false;
		while(true) {
			try {
				Plus.Activities.List userActivities = plusSrv.activities().list(userID, "public");
				userActivities.setMaxResults(100L);
				
				ActivityFeed activityFeed = userActivities.execute();
				numberOfRequests ++;
				
				List<Activity> activities = activityFeed.getItems();
				if(activities == null) {
					isFinished = true;
					break;
				}
				
				for (Activity activity : activities) {
				
					if(activity == null || activity.getId() == null) {
						isFinished = true;
						break;
					}
					
					DateTime publicationTime = activity.getPublished();
					Date publicationDate = new Date(publicationTime.getValue());					
					if(publicationDate.before(sinceDate)) {
						sinceDateReached = true;
						break;
					}

					if(publicationDate.after(sinceDate) && activity != null && activity.getId() != null) {
						GooglePlusPost googlePlusItem = new GooglePlusPost(activity);
						googlePlusItem.setLabel(label);
						
						googlePlusItem.setContributor(streamUser);
						for(Item item : googlePlusItem.getItems()) {
							item.setContributor(streamUser);
						}
							
						posts.add(googlePlusItem);
					}
					else {
						isFinished = true;
						break;
					}
					
				}
				
				if(sinceDateReached) {
					logger.info("Stop retriever. Since date " + sinceDate + " reached for " + userID + " (" + uName + ").");
					break;
				}
				
				if(numberOfRequests > maxRequests) {
					logger.info("Stop retriever. Number of requests (" + numberOfRequests + ") has reached for " + userID + " (" + uName + ").");
					break;
				}
				
				if(activityFeed.getNextPageToken() == null) {
					logger.info("Stop retriever. There is no more pages to fetch for " + userID + " (" + uName + ").");
					break;
				}
				
				if(isFinished) {
					logger.info("Stop retriever. Activity is null for " + userID + " (" + uName + ").");
					break;
				}
				
				userActivities.setPageToken(activityFeed.getNextPageToken());				
			} catch (IOException e) {
				logger.error("#GooglePlus Exception : "+e);
				
				response = getResponse(posts, media, numberOfRequests);
				return response;
			}
		}
		
		response = getResponse(posts, media, numberOfRequests);
		return response;
	}
	
	@Override
	public Response retrieveKeywordsFeed(KeywordsFeed feed, Integer maxRequests) {
		
		Response response = new Response();
		List<Post> posts = new ArrayList<Post>();
		List<Media> media = new ArrayList<Media>();
		
		Date sinceDate = feed.getSinceDate();
		String label = feed.getLabel();
		
		int numberOfRequests = 0;
		
		List<String> keywords = feed.getKeywords();
		
		if(keywords == null || keywords.isEmpty()) {
			logger.info("#GooglePlus : No keywords feed");
			response = getResponse(posts, media, numberOfRequests);
			return response;
		}
		
		String tagsQuery = "";
		for(String key : keywords) {
			String [] words = key.split(" ");
			for(String word : words) {
				if(!tagsQuery.contains(word) && word.length() > 1) {
					tagsQuery += word.toLowerCase() + " ";
				}
			}
		}
		tagsQuery = tagsQuery.trim();
		
		if(tagsQuery.equals("")) {
			response = getResponse(posts, media, numberOfRequests);
			return response;
		}

		
		Map<String, UserAccount> users = new HashMap<String, UserAccount>();
		
		logger.info("Search for (" + tagsQuery + ")");
		boolean isFinished = false, sinceDateReached = false;
		String nextPageToken = null;
		while(true) {
			try {
				Plus.Activities.Search searchActivities = plusSrv.activities().search(tagsQuery);
				searchActivities.setMaxResults(20L);
				searchActivities.setOrderBy("recent");

				if(nextPageToken != null) {
					searchActivities.setPageToken(nextPageToken);
				}
				
				ActivityFeed activityFeed = searchActivities.execute();
				numberOfRequests++;
				
				List<Activity> activities = activityFeed.getItems();
				for (Activity activity : activities) {
					
					DateTime publicationTime = activity.getPublished();
					Date publicationDate = new Date(publicationTime.getValue());
					if(publicationDate.before(sinceDate)) {
						sinceDateReached = true;
						break;
					}
					
					String verb = activity.getVerb();
					String objectType = activity.getObject().getObjectType();
					if(!verb.equals("post") && !verb.equals("share") && !objectType.equals("note") && !objectType.equals("activity")) {
						// unknown type of activity
						logger.info("unknown type of activity: " + verb + " -> " + objectType);
						continue;
					}

					GooglePlusPost googlePlusItem = new GooglePlusPost(activity);
					googlePlusItem.setLabel(label);
					
					String userID = googlePlusItem.getContributor().getUserId();
					logger.info("userID: " + userID);
					
					UserAccount user = null;
					if(userID != null && !users.containsKey(userID)) {
						user = getStreamUser(userID);
						users.put(userID, user);	
					}
					else {
						user = users.get(userID);
					}
							
					if(user != null) {
						googlePlusItem.setContributor(user);
						for(Item item : googlePlusItem.getItems()) {
							item.setContributor(user);
						}
						
						posts.add(googlePlusItem);
					}
		
				 }

				nextPageToken = activityFeed.getNextPageToken();
				
				if(sinceDateReached) {
					logger.info("Stop retriever. Since date " + sinceDate + " reached for (" + tagsQuery + ").");
					break;
				}
				
				if(numberOfRequests > maxRequests) {
					logger.info("Stop retriever. Number of requests (" + numberOfRequests + ") has reached for (" + tagsQuery + ").");
					break;
				}
				
				if(activityFeed.getNextPageToken() == null) {
					logger.info("Stop retriever. There is no more pages to fetch for (" + tagsQuery + ").");
					break;
				}
				
				if(isFinished) {
					logger.info("Stop retriever. Activity is null for (" + tagsQuery + ").");
					break;
				}
				
			} catch (IOException e) {
				logger.error(e);
			}
			
		
		}
		
		response = getResponse(posts, media, numberOfRequests);
		return response;
		
	}
	
	@Override
	public Response retrieveGroupFeed(GroupFeed feed, Integer maxRequests) {
		return new Response();
	}
	
	@Override
	public UserAccount getStreamUser(String uid) {
	
		try {
			People peopleSrv = plusSrv.people();
			Get getRequest = peopleSrv.get(uid);
			Person person = getRequest.execute();
			
			UserAccount streamUser = new GooglePlusAccount(person);
			
			return streamUser;
		} catch (IOException e) {
			logger.error("Exception for user " + uid);
		}
		
		return null;
	}
	
}
