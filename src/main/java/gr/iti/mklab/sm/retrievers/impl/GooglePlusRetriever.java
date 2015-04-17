package gr.iti.mklab.sm.retrievers.impl;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
import com.google.api.services.plus.PlusRequestInitializer;
import com.google.api.services.plus.model.Activity;
import com.google.api.services.plus.model.ActivityFeed;
import com.google.api.services.plus.model.PeopleFeed;
import com.google.api.services.plus.model.Person;

import gr.iti.mklab.simmo.core.UserAccount;
import gr.iti.mklab.simmo.core.documents.Post;
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
 * @author ailiakop
 */
public class GooglePlusRetriever extends SocialMediaRetriever {
	
	private Logger logger = Logger.getLogger(GooglePlusRetriever.class);
	
	private static final HttpTransport transport = new NetHttpTransport();
	private static final JsonFactory jsonFactory = new JacksonFactory();
	
	private Plus plusSrv;
	private String userPrefix = "https://plus.google.com/+";
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
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		
		Date lastItemDate = feed.getSinceDate();
		String label = feed.getLabel();
		
		int numberOfRequests = 0;
		
		boolean isFinished = false;
		
		String uName = feed.getUsername();
		String userID = feed.getId();
		
		if(uName == null && userID == null) {
			logger.info("#GooglePlus : No source feed");
			return response;
		}
				
		//Retrieve userID from Google+
		UserAccount streamUser = null;
		Plus.People.Search searchPeople = null;
		List<Person> people;
		List<Activity> pageOfActivities;
		ActivityFeed activityFeed;
		Plus.Activities.List userActivities;
		
		try {
			if(userID == null) {
				searchPeople = plusSrv.people().search(uName);
				searchPeople.setMaxResults(20L);
				PeopleFeed peopleFeed = searchPeople.execute();
				people = peopleFeed.getItems();
				for(Person person : people) {
					if(person.getUrl().compareTo(userPrefix+uName) == 0) {
						userID = person.getId();
						streamUser = getStreamUser(userID);
						break;
					}
				}
			}
			else {
				streamUser = getStreamUser(userID);
			}
			
			//Retrieve activity with userID
			logger.info("Get public feed of " + userID);
			userActivities = plusSrv.activities().list(userID, "public");
			userActivities.setMaxResults(20L);
			activityFeed = userActivities.execute();
			pageOfActivities = activityFeed.getItems();
			numberOfRequests ++;
			
		} catch (Exception e) {
			logger.error(e);
			return response;
		}
		
		while(pageOfActivities != null && !pageOfActivities.isEmpty()) {
			try {
				for (Activity activity : pageOfActivities) {
				
					DateTime publicationTime = activity.getPublished();
					String PublicationTimeStr = publicationTime.toString();
					String newPublicationTimeStr = PublicationTimeStr.replace("T", " ").replace("Z", " ");
					
					Date publicationDate = null;
					try {
						publicationDate = (Date) formatter.parse(newPublicationTimeStr);
						
					} catch (ParseException e) {
						logger.error("#GooglePlus - ParseException: "+e);
						
						response.setPosts(posts);
						response.setRequests(numberOfRequests);
						return response;
					}
					
					if(publicationDate.after(lastItemDate) && activity != null && activity.getId() != null) {
						GooglePlusPost googlePlusItem = new GooglePlusPost(activity);
						//googlePlusItem.setList(label);
						
						if(streamUser != null) {
							googlePlusItem.setContributor(streamUser);
						}
						
						posts.add(googlePlusItem);
					}
					else {
						isFinished = true;
						break;
					}
					
				}
				numberOfRequests++;
				if(isFinished || numberOfRequests>maxRequests || (activityFeed.getNextPageToken() == null))
					break;
				 
				userActivities.setPageToken(activityFeed.getNextPageToken());
				activityFeed = userActivities.execute();
				pageOfActivities = activityFeed.getItems();
				
			} catch (IOException e) {
				logger.error("#GooglePlus Exception : "+e);
				
				response.setPosts(posts);
				response.setRequests(numberOfRequests);
				return response;
			}
			
			if(isFinished){
				break;
			}
		}

		// The next request will retrieve only items of the last day
		Date dateToRetrieve = new Date(System.currentTimeMillis() - (24*3600*1000));
		feed.setSinceDate(dateToRetrieve);
		
		response.setPosts(posts);
		response.setRequests(numberOfRequests);
		return response;
	}
	
	@Override
	public Response retrieveKeywordsFeed(KeywordsFeed feed, Integer maxRequests) {
		
		Response response = new Response();
		List<Post> posts = new ArrayList<Post>();
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		Date lastItemDate = feed.getSinceDate();
		String label = feed.getLabel();
		
		int totalRequests = 0;
		
		boolean isFinished = false;
		
		List<String> keywords = feed.getKeywords();
		
		if(keywords == null || keywords.isEmpty()) {
			logger.info("#GooglePlus : No keywords feed");
			return response;
		}
		
		String tags = "";
		for(String key : keywords) {
			String [] words = key.split(" ");
			for(String word : words) {
				if(!tags.contains(word) && word.length() > 1)
					tags += word.toLowerCase()+" ";
			}
		}
		
		
		if(tags.equals(""))
			return response;
		
		Plus.Activities.Search searchActivities;
		ActivityFeed activityFeed;
		List<Activity> pageOfActivities;
		try {
			searchActivities = plusSrv.activities().search(tags);
			searchActivities.setMaxResults(20L);
			searchActivities.setOrderBy("recent");
			activityFeed = searchActivities.execute();
			pageOfActivities = activityFeed.getItems();
			totalRequests++;
		} catch (IOException e1) {
			return response;
		}
		
		Map<String, UserAccount> users = new HashMap<String, UserAccount>();
		
		while(pageOfActivities != null && !pageOfActivities.isEmpty()) {
			
			for (Activity activity : pageOfActivities) {
				
				if(activity.getObject().getAttachments() != null){
					
					DateTime publicationTime = activity.getPublished();
					String PublicationTimeStr = publicationTime.toString();
					String newPublicationTimeStr = PublicationTimeStr.replace("T", " ").replace("Z", " ");
					
					Date publicationDate = null;
					try {
						publicationDate = (Date) formatter.parse(newPublicationTimeStr);
						
					} catch (ParseException e) {
						logger.error("#GooglePlus - ParseException: " + e.getMessage());
						response.setPosts(posts);
						response.setRequests(totalRequests);
						return response;
					}
					
					if(publicationDate.after(lastItemDate) && activity != null && activity.getId() != null) {
						GooglePlusPost googlePlusItem = new GooglePlusPost(activity);
						//googlePlusItem.setList(label);
						
						String userID = googlePlusItem.getContributor().getId();
						UserAccount streamUser = null;
						if(userID != null && !users.containsKey(userID)) {
							streamUser = getStreamUser(userID);
							users.put(userID, streamUser);	
						}
						else {
							streamUser = users.get(userID);
						}
						
						if(streamUser != null)
							googlePlusItem.setContributor(streamUser);
						
						posts.add(googlePlusItem);
						
					}
		
				}
		
			 }
			
			 totalRequests++;

			 if(totalRequests>maxRequests || isFinished || (activityFeed.getNextPageToken() == null))
				 break;
			 
			 searchActivities.setPageToken(activityFeed.getNextPageToken());
			 try {
				activityFeed = searchActivities.execute();
			} catch (IOException e) {
				logger.error("GPlus Retriever Exception: " + e.getMessage());
			}
			 pageOfActivities = activityFeed.getItems();
		
		}
		
//		logger.info("#GooglePlus : KeywordsFeed "+feed.getId()+" is done retrieving for this session");
//		logger.info("#GooglePlus : Handler fetched " + items.size() + " posts from " + tags + 
//				" [ " + lastItemDate + " - " + new Date(System.currentTimeMillis()) + " ]");
		
		// The next request will retrieve only items of the last day
		Date dateToRetrieve = new Date(System.currentTimeMillis() - (24*3600*1000));
		feed.setSinceDate(dateToRetrieve);
		
		response.setPosts(posts);
		response.setRequests(totalRequests);
		return response;
		
	}
	
	@Override
	public Response retrieveGroupFeed(GroupFeed feed, Integer maxRequests) {
		return new Response();
	}
	
	@Override
	public UserAccount getStreamUser(String uid) {
		
		People peopleSrv = plusSrv.people();
		try {
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
