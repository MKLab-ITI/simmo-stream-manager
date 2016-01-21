package gr.iti.mklab.sm.retrievers.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import gr.iti.mklab.simmo.core.UserAccount;
import gr.iti.mklab.simmo.core.documents.Post;
import gr.iti.mklab.simmo.core.items.Media;
import gr.iti.mklab.simmo.impl.posts.TwitterPost;
import gr.iti.mklab.simmo.impl.users.TwitterAccount;
import gr.iti.mklab.sm.feeds.Feed;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import twitter4j.Paging;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import gr.iti.mklab.sm.Credentials;
import gr.iti.mklab.sm.feeds.AccountFeed;
import gr.iti.mklab.sm.feeds.GroupFeed;
import gr.iti.mklab.sm.feeds.KeywordsFeed;
import gr.iti.mklab.sm.retrievers.Response;
import gr.iti.mklab.sm.retrievers.SocialMediaRetriever;

/**
 * Class responsible for retrieving Twitter content based on keywords, twitter users or locations
 * The retrieval process takes place through Twitter API (twitter4j)
 * @author Manos Schinas
 */
public class TwitterRetriever extends SocialMediaRetriever {
	
	private Logger  logger = Logger.getLogger(TwitterRetriever.class);
	
	private Twitter twitter = null;
	private TwitterFactory tf = null;
	
	public TwitterRetriever(Credentials credentials) throws Exception {
		
		super(credentials);
		
		if (credentials.getKey() == null || credentials.getSecret() == null ||
				credentials.getAccessToken() == null || credentials.getAccessTokenSecret() == null) {
			logger.error("Twitter requires authentication.");
			throw new Exception("Twitter requires authentication.");
		}
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setJSONStoreEnabled(false)
			.setOAuthConsumerKey(credentials.getKey())
			.setOAuthConsumerSecret(credentials.getSecret())
			.setOAuthAccessToken(credentials.getAccessToken())
			.setOAuthAccessTokenSecret(credentials.getAccessTokenSecret());
		Configuration conf = cb.build();
		
		this.tf = new TwitterFactory(conf);
		twitter = tf.getInstance();
	
	}
	
	@Override
	public Response retrieveAccountFeed(AccountFeed feed, Integer maxRequests) {
		
		if(maxRequests == null) {
			maxRequests = 1;
		}
		
		Response response = new Response();
		List<Post> posts = new ArrayList<Post>();
		List<Media> media = new ArrayList<Media>();
		
		int count = 200;
		
		Integer numberOfRequests = 0;
		
		Date sinceDate = feed.getSinceDate();
		Date newSinceDate = sinceDate;
		
		String feedLabel = feed.getLabel();
		
		Long uid = Long.parseLong(feed.getId());
		String screenName = feed.getUsername();
		if(uid == null && screenName == null) {
			logger.error("Uid and Username is null for feed [" + feed + "]");
			response = getResponse(posts, media, numberOfRequests);
			return response;
		}
		
		int page = 1;
		Paging paging = new Paging(page, count);
		boolean sinceDateReached = false;
		while(true) {
			try {
				ResponseList<Status> responseList = null;
				if(uid != null) {
					responseList = twitter.getUserTimeline(uid, paging);
				}
				else {
					responseList = twitter.getUserTimeline(screenName, paging);	
				}		
				numberOfRequests++;
				
				for(Status status : responseList) {
					if(status != null) {
						
						if(sinceDate != null) {
							Date createdAt = status.getCreatedAt();
							if(newSinceDate.before(createdAt)) {
								newSinceDate = new Date(createdAt.getTime());
							}
							if(sinceDate.after(createdAt)) {
								sinceDateReached = true;
								break;
							}
						}
						
						TwitterPost post = new TwitterPost(status);
						post.setLabel(feedLabel);
						posts.add(post);
					}
				}

				if(numberOfRequests >= maxRequests) {	
					logger.info("Stop retriever. Number of requests (" + numberOfRequests + ") has reached for @" + screenName);
					break;
				}
				if(sinceDateReached) {
					logger.info("Stop retriever. Since date " + sinceDate + " reached for user " + screenName);
					break;
				}
				
				paging.setPage(++page);
			
			} catch (TwitterException e) {
				logger.error(e);
				break;
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
				
		int count = 100;
		int numberOfRequests = 0;

		Date sinceDate = feed.getSinceDate();
		String feedLabel = feed.getLabel();
		
		List<String> keywords = feed.getKeywords();
		if(keywords == null || keywords.isEmpty()) {
			logger.error("#Twitter : No keywords feed");
			response = getResponse(posts, media, numberOfRequests);
			return response;
		}

		//Set the query
		String queryText = StringUtils.join(keywords, " OR ");
		logger.info("Query String: " + queryText);
		Query query = new Query(queryText);
	
		query.count(count);
		query.setResultType(Query.RECENT); //do not set last item date-causes problems!

		boolean sinceDateReached = false;
		try {
			logger.info("Request for " + query);
			
			QueryResult queryResult = twitter.search(query);
			while(queryResult != null) {
				numberOfRequests++;
				
				List<Status> statuses = queryResult.getTweets();
				
				if(statuses == null || statuses.isEmpty()) {
					logger.info("No more results.");	
					break;
				}
				
				logger.info(statuses.size() + " statuses retrieved.");	
				
				for(Status status : statuses) {
					if(status != null) {
						if(sinceDate != null) {
							Date createdAt = status.getCreatedAt();
							if(sinceDate.after(createdAt)) {
								sinceDateReached = true;
								break;
							}
						}
						
						TwitterPost post = new TwitterPost(status);
						post.setLabel(feedLabel);
						posts.add(post);
					}
				}
				
				if(numberOfRequests >= maxRequests) {
					logger.info("numberOfRequests: " + numberOfRequests + " > " + maxRequests);
					break;
				}
				
				if(sinceDateReached) {
					logger.info("Since date reached: " + sinceDate);
					break;
				}
			
				query = queryResult.nextQuery();
				if(query == null) {
					break;
				}
				
				queryResult = twitter.search(query);
			}
			
		} catch (Exception e) {
			logger.error(e.getMessage());
		}	
	
		response = getResponse(posts, media, numberOfRequests);
		return response;
		
	}
	
	@Override
	public Response retrieveGroupFeed(GroupFeed feed, Integer maxRequests) {
		
		Response response = new Response();
		List<Post> posts = new ArrayList<Post>();
		
		Integer numberOfRequests = 0;
			
		String ownerScreenName = feed.getGroupCreator();
		String slug = feed.getGroupId();
				
		int page = 1;
		Paging paging = new Paging(page, 200);
		while(true) {
			try {
				numberOfRequests++;
				ResponseList<Status> responseList = twitter.getUserListStatuses(ownerScreenName, slug, paging);
				for(Status status : responseList) {
					if(status != null) {
						TwitterPost twitterItem = new TwitterPost(status);
						posts.add(twitterItem);
					}
				}
					
				paging.setPage(++page);
			} catch (TwitterException e) {
				logger.error(e);	
				break;
			}
		}
		
		response.setRequests(numberOfRequests);
		response.setPosts(posts);
		return response;
	}

	@Override
	public UserAccount getStreamUser(String uid) {
		try {
			long userId = Long.parseLong(uid);
			User user = twitter.showUser(userId);
			
			UserAccount userAccount = new TwitterAccount(user);
			return userAccount;
		}
		catch(Exception e) {
			logger.error(e);
			return null;
		}
	}


	public static void main(String...args) throws Exception {
		
		Credentials credentials = new Credentials ();
		credentials.setKey("");
		credentials.setSecret("");
		credentials.setAccessToken("");
		credentials.setAccessTokenSecret("");
		
		TwitterRetriever retriever = new TwitterRetriever(credentials);
	
		Date since = new Date(System.currentTimeMillis()-9600000);
		Feed feed = new AccountFeed("398789134", null, since);
		//Feed feed = new KeywordsFeed("1", "obama", since);
		
		
		Response response = retriever.retrieve(feed);
		System.out.println(response.getNumberOfPosts() + " posts found!");
		
		for(Post post : response.getPosts()) {
			System.out.println(post.getId());
			System.out.println(post.getContributor().getUsername());
			System.out.println(post.getContributor().getId());
			System.out.println(post.getTitle());
			System.out.println(post.getType());
			System.out.println(post.getCreationDate());
			System.out.println(post.getLanguage());
			System.out.println("==============================");
		}
		
	}
	
}
