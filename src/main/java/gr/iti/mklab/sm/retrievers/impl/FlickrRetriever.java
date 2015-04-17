package gr.iti.mklab.sm.retrievers.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.flickr4java.flickr.Flickr;
import com.flickr4java.flickr.REST;
import com.flickr4java.flickr.people.PeopleInterface;
import com.flickr4java.flickr.people.User;
import com.flickr4java.flickr.photos.Extras;
import com.flickr4java.flickr.photos.Photo;
import com.flickr4java.flickr.photos.PhotoList;
import com.flickr4java.flickr.photos.PhotosInterface;
import com.flickr4java.flickr.photos.SearchParameters;

import gr.iti.mklab.simmo.core.UserAccount;
import gr.iti.mklab.simmo.core.documents.Post;
import gr.iti.mklab.simmo.impl.posts.FlickrPost;
import gr.iti.mklab.simmo.impl.users.FlickrAccount;
import gr.iti.mklab.sm.Credentials;
import gr.iti.mklab.sm.feeds.AccountFeed;
import gr.iti.mklab.sm.feeds.GroupFeed;
import gr.iti.mklab.sm.feeds.KeywordsFeed;
import gr.iti.mklab.sm.retrievers.Response;
import gr.iti.mklab.sm.retrievers.SocialMediaRetriever;


/**
 * Class responsible for retrieving Flickr content based on keywords,users or location coordinates
 * The retrieval process takes place through Flickr API.
 * @author ailiakop
 */
public class FlickrRetriever extends SocialMediaRetriever {

	private Logger logger = Logger.getLogger(FlickrRetriever.class);
	
	private static final int PER_PAGE = 500;
	
	private String flickrKey;
	private String flickrSecret;

	private Flickr flickr;

	private HashMap<String, UserAccount> userMap;
	

	public FlickrRetriever(Credentials credentials) {
		super(credentials);
		
		this.flickrKey = credentials.getKey();
		this.flickrSecret = credentials.getSecret();
		
		userMap = new HashMap<String, UserAccount>();
		
		Flickr.debugStream = false;
		
		this.flickr = new Flickr(flickrKey, flickrSecret, new REST());
	}
	
	@Override
	public Response retrieveAccountFeed(AccountFeed feed, Integer maxRequests) {
		
		Response response = new Response();
		List<Post> items = new ArrayList<Post>();
		
		Date dateToRetrieve = feed.getSinceDate();
		String label = feed.getLabel();
		
		int page=1, pages=1; //pagination
		int numberOfRequests = 0;
		int numberOfResults = 0;
		
		//Here we search the user by the userId given (NSID) - 
		// however we can get NSID via flickrAPI given user's username
		String userID = feed.getId();
		
		if(userID == null) {
			logger.info("#Flickr : No source feed");
			return response;
		}
		
		PhotosInterface photosInteface = flickr.getPhotosInterface();
		SearchParameters params = new SearchParameters();
		params.setUserId(userID);
		params.setMinUploadDate(dateToRetrieve);
		
		Set<String> extras = new HashSet<String>(Extras.ALL_EXTRAS);
		extras.remove(Extras.MACHINE_TAGS);
		params.setExtras(extras);
		
		while(page<=pages && numberOfRequests<=maxRequests) {
			
			PhotoList<Photo> photos;
			try {
				numberOfRequests++;
				photos = photosInteface.search(params , PER_PAGE, page++);
			} catch (Exception e) {
				break;
			}
			
			pages = photos.getPages();
			numberOfResults += photos.size();

			if(photos.isEmpty()) {
				break;
			}
		
			for(Photo photo : photos) {

				String userid = photo.getOwner().getId();
				UserAccount streamUser = userMap.get(userid);
				if(streamUser == null) {
					streamUser = getStreamUser(userid);
					userMap.put(userid, streamUser);
				}

				FlickrPost flickrItem = new FlickrPost(photo);
				//flickrItem.setList(label);
				
				items.add(flickrItem);
			}
		}
		
		//logger.info("#Flickr : Done retrieving for this session");
//		logger.info("#Flickr : Handler fetched " + items.size() + " photos from " + userID + 
//				" [ " + lastItemDate + " - " + new Date(System.currentTimeMillis()) + " ]");


		response.setPosts(items);
		response.setRequests(numberOfRequests);
		return response;
	}
	
	@Override
	public Response retrieveKeywordsFeed(KeywordsFeed feed, Integer maxRequests) {
		
		Response response = new Response();
		List<Post> items = new ArrayList<Post>();
		
		Date dateToRetrieve = feed.getSinceDate();
		String label = feed.getLabel();
		
		int page=1, pages=1;
		
		int numberOfRequests = 0;
		int numberOfResults = 0;
		
		List<String> keywords = feed.getKeywords();
		
		if(keywords == null || keywords.isEmpty()) {
			logger.error("#Flickr : Text is emtpy");
			return response;
		}
		
		List<String> tags = new ArrayList<String>();
		String text = "";
		for(String key : keywords) {
			String [] words = key.split("\\s+");
			for(String word : words) {
				if(!tags.contains(word) && word.length()>1) {
					tags.add(word);
					text += (word + " ");
				}
			}
		}
		
		
		if(text.equals("")) {
			logger.error("#Flickr : Text is emtpy");
			response.setPosts(items);
			response.setRequests(numberOfRequests);
			return response;
		}
		
		PhotosInterface photosInteface = flickr.getPhotosInterface();
		SearchParameters params = new SearchParameters();
		params.setText(text);
		params.setMinUploadDate(dateToRetrieve);
		
		Set<String> extras = new HashSet<String>(Extras.ALL_EXTRAS);
		extras.remove(Extras.MACHINE_TAGS);
		params.setExtras(extras);
		
		while(page<=pages && numberOfRequests<=maxRequests ) {
			
			PhotoList<Photo> photos;
			try {
				numberOfRequests++;
				photos = photosInteface.search(params , PER_PAGE, page++);
			} catch (Exception e) {
				logger.error("Exception: " + e.getMessage());
				continue;
			}
			
			pages = photos.getPages();
			numberOfResults += photos.size();

			if(photos.isEmpty()) {
				break;
			}
		
			for(Photo photo : photos) {

				String userid = photo.getOwner().getId();
				UserAccount streamUser = userMap.get(userid);
				if(streamUser == null) {
					streamUser = getStreamUser(userid);
					userMap.put(userid, streamUser);
				}

				FlickrPost flickrItem = new FlickrPost(photo);
				//flickrItem.setList(label);
				
				items.add(flickrItem);
			}
		}
			
//		logger.info("#Flickr : Done retrieving for this session");
//		logger.info("#Flickr : Handler fetched " + items.size() + " photos from " + text + 
//				" [ " + lastItemDate + " - " + new Date(System.currentTimeMillis()) + " ]");

		
		response.setPosts(items);
		response.setRequests(numberOfRequests);
		return response;
	}

	/*
	public List<Post> retrieveLocationFeed(LocationFeed feed, Integer maxResults, Integer maxRequests){
		
		List<Post> items = new ArrayList<Post>();
		
		Date dateToRetrieve = feed.getDateToRetrieve();
		String label = feed.getLabel();
		
		Double[][] bbox = feed.getLocation().getbbox();
		
		if(bbox == null || bbox.length==0)
			return items;
		
		int page=1, pages=1;
		int numberOfRequests = 0;
		int numberOfResults = 0;
		
		PhotosInterface photosInteface = flickr.getPhotosInterface();
		SearchParameters params = new SearchParameters();
		params.setBBox(bbox[0][0].toString(), bbox[0][1].toString(), bbox[1][0].toString(), bbox[1][1].toString());
		params.setMinUploadDate(dateToRetrieve);
		
		Set<String> extras = new HashSet<String>(Extras.ALL_EXTRAS);
		extras.remove(Extras.MACHINE_TAGS);
		params.setExtras(extras);
		
		while(page<=pages && numberOfRequests<=maxRequests && numberOfResults<=maxResults ) {
			
			PhotoList<Photo> photos;
			try {
				photos = photosInteface.search(params , PER_PAGE, page++);
			} catch (FlickrException e) {
				break;
			}
			
			pages = photos.getPages();
			numberOfResults += photos.size();

			if(photos.isEmpty()) {
				break;
			}
		
			for(Photo photo : photos) {

				String userid = photo.getOwner().getId();
				UserAccount streamUser = userMap.get(userid);
				if(streamUser == null) {
					streamUser = getStreamUser(userid);

					userMap.put(userid, streamUser);
				}

				FlickrPost flickrItem = new FlickrPost(photo);
				//flickrItem.setList(label);
				
				items.add(flickrItem);
			}
		}
		
		logger.info("#Flickr : Handler fetched " + items.size() + " photos "+ 
				" [ " + dateToRetrieve + " - " + new Date(System.currentTimeMillis()) + " ]");
		
		return items;
    }
	*/
	
	@Override
	public Response retrieveGroupFeed(GroupFeed feed, Integer maxRequests) {
		return null;
	}
	
	@Override
	public UserAccount getStreamUser(String uid) {
		try {
			PeopleInterface peopleInterface = flickr.getPeopleInterface();
			User user = peopleInterface.getInfo(uid);
			
			UserAccount streamUser = new FlickrAccount(user);
			return streamUser;
		}
		catch(Exception e) {
			return null;
		}
		
	}
	
}
