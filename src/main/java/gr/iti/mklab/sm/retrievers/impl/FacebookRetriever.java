package gr.iti.mklab.sm.retrievers.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.restfb.Connection;
import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.Parameter;
import com.restfb.exception.FacebookNetworkException;
import com.restfb.exception.FacebookResponseStatusException;
import com.restfb.types.CategorizedFacebookType;
import com.restfb.types.Comment;
import com.restfb.types.Page;
import com.restfb.types.User;

import gr.iti.mklab.simmo.core.UserAccount;
import gr.iti.mklab.simmo.core.documents.Post;
import gr.iti.mklab.simmo.impl.posts.FacebookPost;
import gr.iti.mklab.simmo.impl.users.FacebookAccount;
import gr.iti.mklab.sm.Credentials;
import gr.iti.mklab.sm.feeds.AccountFeed;
import gr.iti.mklab.sm.feeds.GroupFeed;
import gr.iti.mklab.sm.feeds.KeywordsFeed;
import gr.iti.mklab.sm.retrievers.Response;
import gr.iti.mklab.sm.retrievers.SocialMediaRetriever;

/**
 * Class responsible for retrieving facebook content based on keywords or facebook users/facebook pages
 * The retrieval process takes place through facebook graph API.
 * @author ailiakop
 * 
 */
public class FacebookRetriever extends SocialMediaRetriever {
			
	private FacebookClient facebookClient;
	
	private Logger  logger = Logger.getLogger(FacebookRetriever.class);
	
	public FacebookRetriever(Credentials credentials) throws Exception {
		super(credentials);
		
		String accessToken = credentials.getAccessToken();
		if(accessToken == null) {
			if (credentials.getKey() == null && credentials.getSecret() == null) {
				logger.error("Facebook requires authentication.");
				throw new Exception("Facebook requires authentication.");
			}
			accessToken = credentials.getKey() + "|" + credentials.getSecret();
		}
		
		facebookClient = new DefaultFacebookClient(accessToken);
	}

	@Override
	public Response retrieveAccountFeed(AccountFeed feed, Integer maxRequests) {
		
		Response response = new Response();
		List<Post> items = new ArrayList<Post>();

		Integer totalRequests = 0;
		
		Date lastItemDate = feed.getSinceDate();
		String label = feed.getLabel();
		
		boolean isFinished = false;
		
		String userName = feed.getUsername();
		if(userName == null) {
			logger.error("#Facebook : No source feed");
			return response;
		}
		String userFeed = userName+"/feed";
		
		Connection<com.restfb.types.Post> connection;
		User page;
		try {
			connection = facebookClient.fetchConnection(userFeed , com.restfb.types.Post.class);
			page = facebookClient.fetchObject(userName, User.class);
		}
		catch(Exception e) {
			return response;
		}
		
		FacebookAccount facebookUser = new FacebookAccount(page);
		for(List<com.restfb.types.Post> connectionPage : connection) {	
			totalRequests++;
			for(com.restfb.types.Post post : connectionPage) {	
				
				Date publicationDate = post.getCreatedTime();
				
				if(publicationDate.after(lastItemDate) && post!=null && post.getId() != null) {
					FacebookPost facebookPost = new FacebookPost(post, facebookUser);
					//facebookUpdate.setList(label);
					
					items.add(facebookPost);
					
					com.restfb.types.Post.Comments comments = post.getComments();
				    if(comments != null) {
				    	for(Comment comment : comments.getData()) {
			    			FacebookPost facebookComment = new FacebookPost(comment, post, null);
			    			//facebookComment.setList(label);
			    			
			    			items.add(facebookComment);
			    		} 
				    }
				 }
				
				if(publicationDate.before(lastItemDate) || totalRequests>maxRequests){
					isFinished = true;
					break;
				}
			
			}
			if(isFinished)
				break;
			
		}

		logger.info("Facebook: " + items.size() + " posts from " + userFeed + " [ " + lastItemDate + " - " + new Date(System.currentTimeMillis()) + " ]");
		
		response.setPosts(items);
		response.setRequests(totalRequests);
		return response;
	}
	
	@Override
	public Response retrieveKeywordsFeed(KeywordsFeed feed, Integer maxRequests) {
		
		Response response = new Response();
		List<Post> items = new ArrayList<Post>();
		
		Date lastItemDate = feed.getSinceDate();
		String label = feed.getLabel();
		
		boolean isFinished = false;
		
		List<String> keywords = feed.getKeywords();
		
		if(keywords == null || keywords.isEmpty()) {
			logger.error("#Facebook : No keywords feed");
			return response;
		}

		String tags = "";
		for(String keyword : keywords) {
			String [] words = keyword.split(" ");
			for(String word : words) {
				if(!tags.contains(word) && word.length() > 1) {
					tags += word.toLowerCase()+" ";
				}
			}
		}
		
		if(tags.equals(""))
			return response;
		
		Connection<com.restfb.types.Post> connection = null;
		try {
			connection = facebookClient.fetchConnection("search", com.restfb.types.Post.class, Parameter.with("q", tags), Parameter.with("type", "post"));
		}catch(FacebookResponseStatusException e) {
			logger.error(e.getMessage());
			return response;
		}
		catch(Exception e) {
			logger.error(e.getMessage());
			return response;
		}
		
		try {
			for(List<com.restfb.types.Post> connectionPage : connection) {
				for(com.restfb.types.Post post : connectionPage) {	
					
					Date publicationDate = post.getCreatedTime();
					try {
						if(publicationDate.after(lastItemDate) && post!=null && post.getId()!=null) {
							
							FacebookPost fbItem;
							
							//Get the user of the post
							CategorizedFacebookType cUser = post.getFrom();
							if(cUser != null) {
								User user = facebookClient.fetchObject(cUser.getId(), User.class);
								FacebookAccount facebookUser = new FacebookAccount(user);
								
								fbItem = new FacebookPost(post, facebookUser);
								//fbItem.setList(label);
							}
							else {
								fbItem = new FacebookPost(post);
								//fbItem.setList(label);
							}
							
							items.add(fbItem);
						}
					}
					catch(Exception e) {
						logger.error(e.getMessage());
						break;
					}
					
					if(publicationDate.before(lastItemDate)) {
						isFinished = true;
						break;
					}
					
				}
				if(isFinished)
					break;
				
			}
		}
		catch(FacebookNetworkException e) {
			logger.error(e.getMessage());
		}
		
		logger.info("Facebook: " + items.size() + " posts for " + tags + " [ " + lastItemDate + " - " + new Date(System.currentTimeMillis()) + " ]");
		
		response.setPosts(items);
		return response;
	}
	
	@Override
	public Response retrieveGroupFeed(GroupFeed feed, Integer maxRequests) {
		return new Response();
	}

	/*
	public MediaItem getMediaItem(String mediaId) {
		Photo photo = facebookClient.fetchObject(mediaId, Photo.class);
		
		if(photo == null)
			return null;

		MediaItem mediaItem = null;
		try {
			String src = photo.getSource();
			mediaItem = new MediaItem(new URL(src));
			mediaItem.setId("Facebook#" + photo.getId());
			
			mediaItem.setPageUrl(photo.getLink());
			mediaItem.setThumbnail(photo.getPicture());
			
			mediaItem.setSource("Facebook");
			mediaItem.setType("image");
			
			mediaItem.setTitle(photo.getName());
			
			Date date = photo.getCreatedTime();
			mediaItem.setPublicationTime(date.getTime());
			
			mediaItem.setSize(photo.getWidth(), photo.getHeight());
			mediaItem.setLikes((long) photo.getLikes().size());
			
			CategorizedFacebookType from = photo.getFrom();
			if(from != null) {
				UserAccount streamUser = new FacebookAccount(from);
				mediaItem.setUser(streamUser);
				mediaItem.setUserId(streamUser.getUserid());
			}
			
			
		} catch (MalformedURLException e) {
			logger.error(e);
		}
		
		return mediaItem;
	}
	*/
	
	@Override
	public UserAccount getStreamUser(String uid) {
		try {
			Page page = facebookClient.fetchObject(uid, Page.class);
			UserAccount facebookUser = new FacebookAccount(page);
			return facebookUser;
		}
		catch(Exception e) {
			logger.error(e);
			return null;
		}
	}

}
