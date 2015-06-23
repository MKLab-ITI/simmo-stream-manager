package gr.iti.mklab.sm.retrievers.impl;

import java.net.MalformedURLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gr.iti.mklab.simmo.core.UserAccount;
import gr.iti.mklab.simmo.core.documents.Post;
import gr.iti.mklab.simmo.impl.posts.TumblrPost;
import gr.iti.mklab.simmo.impl.users.TumblrAccount;
import gr.iti.mklab.sm.Credentials;
import gr.iti.mklab.sm.feeds.AccountFeed;
import gr.iti.mklab.sm.feeds.GroupFeed;
import gr.iti.mklab.sm.feeds.KeywordsFeed;
import gr.iti.mklab.sm.retrievers.Response;
import gr.iti.mklab.sm.retrievers.SocialMediaRetriever;

import org.apache.log4j.Logger;
import org.scribe.exceptions.OAuthConnectionException;

import com.tumblr.jumblr.JumblrClient;
import com.tumblr.jumblr.exceptions.JumblrException;
import com.tumblr.jumblr.types.Blog;


/**
 * Class responsible for retrieving Tumblr content based on keywords or tumblr users
 * The retrieval process takes place through Tumblr API (Jumblr)
 * @author ailiakop
 */
public class TumblrRetriever extends SocialMediaRetriever {
	
	private Logger logger = Logger.getLogger(TumblrRetriever.class);
	
	private JumblrClient client;
	
	public TumblrRetriever(Credentials credentials) throws Exception {
		super(credentials);
		if (credentials.getKey() == null || credentials.getSecret() == null) {
			logger.error("Tumblr requires authentication.");
			throw new Exception("Tumblr requires authentication.");
		}
		
		client = new JumblrClient(credentials.getKey(), credentials.getSecret());
	}

	
	@Override
	public Response retrieveAccountFeed(AccountFeed feed, Integer maxRequests) {
		
		Response response = new Response();
		List<Post> posts = new ArrayList<Post>();
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		Date lastItemDate = feed.getSinceDate();
		
		int numberOfRequests = 0;
		
		boolean isFinished = false;
		
		String uName = feed.getUsername();
		if(uName == null){
			logger.info("#Tumblr : No source feed");
			return null;
		}
		
		Blog blog = client.blogInfo(uName);
		TumblrAccount tumblrStreamUser = new TumblrAccount(blog);
		List<com.tumblr.jumblr.types.Post> tumblrPosts;
		Map<String,String> options = new HashMap<String,String>();
		
		Integer offset = 0;
		Integer limit = 20;
		options.put("limit", limit.toString());
	
		while(true){
			
			options.put("offset", offset.toString());
			
			tumblrPosts = blog.posts(options);
			if(posts == null || posts.isEmpty())
				break;
			
			numberOfRequests ++;
			
			for(com.tumblr.jumblr.types.Post post : tumblrPosts) {
				
				if(post.getType().equals("photo") || post.getType().equals("video") || post.getType().equals("link")){
					
					String retrievedDate = post.getDateGMT().replace(" GMT", "");
					retrievedDate+=".0";
					
					Date publicationDate = null;
					try {
						publicationDate = (Date) formatter.parse(retrievedDate);
						
					} catch (ParseException e) {
						response.setRequests(numberOfRequests);
						response.setPosts(posts);
						return response;
					}
					
					if(publicationDate.after(lastItemDate) && post != null && post.getId() != null){
						
						TumblrPost tumblrPost = null;
						try {
							tumblrPost = new TumblrPost(post,tumblrStreamUser);
						} catch (MalformedURLException e) {
							response.setRequests(numberOfRequests);
							response.setPosts(posts);
							return response;
						}
						
						posts.add(tumblrPost);
						
					}
				
				}
				if(numberOfRequests>maxRequests){
					isFinished = true;
					break;
				}
			}
			if(isFinished)
				break;
			
			offset+=limit;
		}

		//logger.info("#Tumblr : Done retrieving for this session");
//		logger.info("#Tumblr : Handler fetched " +totalRetrievedItems + " posts from " + uName + 
//				" [ " + lastItemDate + " - " + new Date(System.currentTimeMillis()) + " ]");
		
		response.setRequests(numberOfRequests);
		response.setPosts(posts);
		return response;
	}
	
	@Override
	public Response retrieveKeywordsFeed(KeywordsFeed feed, Integer maxRequests) {
		
		Response response = new Response();
		List<Post> posts = new ArrayList<Post>();
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		Date currentDate = new Date(System.currentTimeMillis());
		Date indexDate = currentDate;
		Date lastItemDate = feed.getSinceDate();
		DateUtil dateUtil = new DateUtil();
		
		int numberOfRequests=0;
		
		boolean isFinished = false;
		
		List<String> keywords = feed.getKeywords();
		
		if(keywords == null || keywords.isEmpty()) {
			logger.info("#Tumblr : No keywords feed");
			return response;
		}
		
		String tags = "";
		for(String key : keywords) {
			String [] words = key.split("\\s+");
			for(String word : words) {
				if(!tags.contains(word) && word.length()>1) {
					tags += word.toLowerCase()+" ";
				}
			}
		}
		
		
		if(tags.equals(""))
			return response;
		
		while(indexDate.after(lastItemDate) || indexDate.equals(lastItemDate)){
			
			Map<String,String> options = new HashMap<String,String>();
			Long checkTimestamp = indexDate.getTime();
			Integer check = checkTimestamp.intValue();
			options.put("featured_timestamp", check.toString());
			List<com.tumblr.jumblr.types.Post> tumblrPosts;
			try{
				tumblrPosts = client.tagged(tags);
			}catch(JumblrException e){
				return response;
			}catch(OAuthConnectionException e1){
				return response;
			}
			
			if(tumblrPosts == null || tumblrPosts.isEmpty())
				break;
			
			numberOfRequests ++;
			
			for(com.tumblr.jumblr.types.Post post : tumblrPosts) {
				
				if(post.getType().equals("photo") || post.getType().equals("video") ||  post.getType().equals("link")) {
					
					String retrievedDate = post.getDateGMT().replace(" GMT", "");
					retrievedDate+=".0";
					Date publicationDate = null;
					try {
						publicationDate = (Date) formatter.parse(retrievedDate);
						
					} catch (ParseException e) {
						response.setRequests(numberOfRequests);
						response.setPosts(posts);
						return response;
					}
					
					if(post != null && post.getId() != null){
						//Get the blog
						String blogName = post.getBlogName();
						Blog blog = client.blogInfo(blogName);
						TumblrAccount tumblrStreamUser = new TumblrAccount(blog);
						
						TumblrPost tumblrItem = null;
						try {
							tumblrItem = new TumblrPost(post, tumblrStreamUser);
						} catch (MalformedURLException e) {
							response.setRequests(numberOfRequests);
							response.setPosts(posts);
							return response;
						}
						
						if(tumblrItem != null){
							posts.add(tumblrItem);
						}
					}
				
				}
				
				if(numberOfRequests>=maxRequests) {
					isFinished = true;
					break;
				}
			}
			
			if(isFinished)
				break;
			
			indexDate = dateUtil.addDays(indexDate, -1);
				
		}
		
		response.setRequests(numberOfRequests);
		response.setPosts(posts);
		return response;
		
	}

	@Override
	public Response retrieveGroupFeed(GroupFeed feed, Integer maxRequests) {
		return new Response();
	}

	public class DateUtil
	{
	    public Date addDays(Date date, int days)
	    {
	        Calendar cal = Calendar.getInstance();
	        cal.setTime(date);
	        cal.add(Calendar.DATE, days); //minus number decrements the days
	        return cal.getTime();
	    }
	}



	@Override
	public UserAccount getStreamUser(String uid) {
		return null;
	}

}
