package gr.iti.mklab.sm.retrievers.impl;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.jinstagram.Instagram;
import org.jinstagram.InstagramOembed;
import org.jinstagram.exceptions.InstagramException;
import org.jinstagram.entity.common.Pagination;
import org.jinstagram.entity.oembed.OembedInformation;
import org.jinstagram.entity.tags.TagMediaFeed;
import org.jinstagram.entity.users.basicinfo.UserInfo;
import org.jinstagram.entity.users.feed.MediaFeed;
import org.jinstagram.entity.users.feed.MediaFeedData;
import org.jinstagram.entity.users.feed.UserFeed;
import org.jinstagram.entity.users.feed.UserFeedData;
import org.jinstagram.auth.model.Token;

import gr.iti.mklab.simmo.core.UserAccount;
import gr.iti.mklab.simmo.core.documents.Post;
import gr.iti.mklab.simmo.core.items.Media;
import gr.iti.mklab.simmo.impl.posts.InstagramPost;
import gr.iti.mklab.simmo.impl.users.InstagramAccount;
import gr.iti.mklab.sm.Credentials;
import gr.iti.mklab.sm.feeds.AccountFeed;
import gr.iti.mklab.sm.feeds.GroupFeed;
import gr.iti.mklab.sm.feeds.KeywordsFeed;
import gr.iti.mklab.sm.retrievers.Response;
import gr.iti.mklab.sm.retrievers.SocialMediaRetriever;

/**
 * Class responsible for retrieving Instagram content based on keywords or instagram users or locations
 * The retrieval process takes place through Instagram API
 * @author manosetro
 */
public class InstagramRetriever extends SocialMediaRetriever {
	
	private Logger logger = Logger.getLogger(InstagramRetriever.class);
	
	private Instagram instagram = null;
	private InstagramOembed instagramOembed;
	
	public InstagramRetriever(Credentials credentials) throws Exception {
		super(credentials);
		
		if (credentials.getKey() == null || credentials.getSecret() == null 
				|| credentials.getAccessToken() == null) {
			logger.error("Instagram requires authentication.");
			throw new Exception("Instagram requires authentication.");
		}
		
		Token accessToken = new Token(credentials.getAccessToken(), credentials.getAccessTokenSecret()); 
		this.instagram = new Instagram(credentials.getKey());
		this.instagram.setAccessToken(accessToken);
		this.instagramOembed = new InstagramOembed();
	}
	
	@Override
	public Response retrieveAccountFeed(AccountFeed feed, Integer maxRequests) {
		
		Response response = new Response();
		List<Post> posts = new ArrayList<Post>();
		List<Media> media = new ArrayList<Media>();
		
		Date sinceDate = feed.getSinceDate();
		String label = feed.getLabel();
		
		int numberOfRequests = 0;
		int count = 100;
		
		String uid = feed.getId();
		String uName = feed.getUsername();
		if(uid == null && uName == null) {
			logger.error("#Instagram : No source feed");
			response = getResponse(posts, media, numberOfRequests);
			return response;
		}
			
		logger.info("#Instagram : Retrieving User Feed : " + uName);
		  
		UserAccount user = null;
		try {
			if(uid != null) {
				UserInfo userInfo = instagram.getUserInfo(uid);
				user = new InstagramAccount(userInfo);
			}
			else {
				UserFeed userf = instagram.searchUser(uName);
				List<UserFeedData> usersFeed = userf.getUserList();
				for(UserFeedData userFeed : usersFeed) {
					if(userFeed.getUserName().equals(uName)) {
						user = new InstagramAccount(userFeed);
						break;
					}
				}
			}	
		}
		catch(InstagramException e) {
			logger.error("#Instagram Exception for feed (" + feed.getId() + ")", e);
			response = getResponse(posts, media, numberOfRequests);
			return response;
		}
		
		try {
			boolean sinceDateReached = false;
			
			MediaFeed mediaFeed = instagram.getRecentMediaFeed(user.getUserId(), count, null, null, null, sinceDate);
			while(true) {
				if(mediaFeed == null) {
					break;
				}
					
				for(MediaFeedData mfeed : mediaFeed.getData()) {
					int createdTime = Integer.parseInt(mfeed.getCreatedTime());
					Date publicationDate = new Date((long) createdTime * 1000);
						
					if(sinceDate.after(publicationDate)) {
						sinceDateReached = true;
						break;
					}
						
					if(mfeed != null && mfeed.getId() != null) {
						InstagramPost instagramItem = new InstagramPost(mfeed);
                        instagramItem.setLabel(label);
								
						posts.add(instagramItem);
					}
				}
					
				Pagination pagination = mediaFeed.getPagination();
				if(pagination == null || !pagination.hasNextPage()) {
					logger.info("Stop retriever. There is no next page for user (" + user.getUsername() + ")");
					break;
				}
					
				if(numberOfRequests >= maxRequests) {
					logger.info("Stop retriever. Number of requests (" + numberOfRequests + ") has reached for user (" + user.getUsername() + ")");
					break;
				}
		        	
				if(sinceDateReached) {
					logger.info("Stop retriever. Since date " + sinceDate + " reached for user (" + user.getUsername() + ")");
					break;
				}
					
				numberOfRequests++;
				mediaFeed = instagram.getRecentMediaNextPage(pagination);
			}
				
		}
		catch(InstagramException e) {
			logger.error("#Instagram Exception for feed (" + feed.getId() + ")", e);	
			response = getResponse(posts, media, numberOfRequests);
			return response;
		} 
		catch (MalformedURLException e) {
			logger.error("#Instagram Exception for (" + feed.getId() + ")", e);
			response = getResponse(posts, media, numberOfRequests);
			return response;
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
			logger.error("#Instagram : No keywords for feed (" + feed.getId() + ")");
			response = getResponse(posts, media, numberOfRequests);
			return response;
		}
		
		String tags = "";
		for(String key : keywords) {
			String [] words = key.split(" ");
			for(String word : words) {
				if(!tags.contains(word) && word.length() > 1) {
					tags += word.toLowerCase();
				}
			}
		}
		
		tags = tags.replaceAll(" ", "");
		tags = tags.trim();
		
		if(tags.equals("")) {
			logger.error("#Instagram : No keywords feed for query (" + tags + ")");
			response = getResponse(posts, media, numberOfRequests);
			return response;
		}
		
		
		boolean sinceDateReached = false;
		TagMediaFeed tagFeed = null;
		try {
			numberOfRequests++;
			tagFeed = instagram.getRecentMediaTags(tags, 50l);
		}
		catch(InstagramException e) {
			logger.error("Instagram retriever exception for (" + feed.getId() + ")", e);
			
			response = getResponse(posts, media, numberOfRequests);
			return response;
		}
		
		while(true) {
			try {
				if(tagFeed == null) { 
					logger.info("Stop retriever. Tag feed not found for query (" + tags + ")");
					break;
				}
				
				for(MediaFeedData mediaData : tagFeed.getData()) {
					int createdTime = Integer.parseInt(mediaData.getCreatedTime());
					Date publicationDate = new Date((long) createdTime * 1000);
					
					if(publicationDate.before(sinceDate)) {
						sinceDateReached = true;
						break;
					}

					InstagramPost instagramItem = new InstagramPost(mediaData);
                    instagramItem.setLabel(label);
                    posts.add(instagramItem);
				}
				
				Pagination pagination = tagFeed.getPagination();
				
				if(pagination==null || !pagination.hasNextPage()) {
					logger.info("Stop retriever. There is no next page for query (" + tags + ")");
					break;
				}
				
	        	if(numberOfRequests >= maxRequests) {
	        		logger.info("Stop retriever. Number of requests (" + numberOfRequests + ") has reached for query (" + tags + ")");
					break;
				}
	        	
				if(sinceDateReached) {
					logger.info("Stop retriever. Since date " + sinceDate + " reached for query (" + tags + ")");
					break;
				}
				
				numberOfRequests++;
				tagFeed = instagram.getTagMediaInfoNextPage(pagination);
				
			}
			catch(InstagramException e) {
				logger.error("Instagram retriever exception for (" + feed.getId() + ")", e);
				
				response.setRequests(numberOfRequests);
				return response;
			} catch (MalformedURLException e) {
				logger.error("Instagram retriever exception for (" + feed.getId() + ")", e);
				
				response = getResponse(posts, media, numberOfRequests);
				return response;
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
			UserInfo userInfo = instagram.getUserInfo(uid);
			
			UserAccount user = new InstagramAccount(userInfo.getData());
			return user;
		}
		catch(Exception e) {
			logger.error("Exception for user " + uid + " => " + e.getMessage());
			return null;
		}
	}
	
	private String getMediaId(String url) {
		try {
			OembedInformation info = instagramOembed.getOembedInformation(url);
			if(info == null) 
				return null;
			return info.getMediaId();
		} catch (Exception e) {
			logger.error("Failed to get id for " + url + " => " + e.getMessage());
		}
		return null;
	}

}
