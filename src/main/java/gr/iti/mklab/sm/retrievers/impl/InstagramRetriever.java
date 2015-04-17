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
	private boolean loggingEnabled = false;
	
	private Instagram instagram = null;

	private MediaFeed mediaFeed = new MediaFeed();
	private TagMediaFeed tagFeed = new TagMediaFeed();

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
		
		Date lastItemDate = feed.getSinceDate();
		String label = feed.getLabel();
		
		int numberOfRequests = 0;
	
		String uName = feed.getUsername();
		if(uName == null) {
			logger.error("#Instagram : No source feed");
			return response;
		}
			
		if(loggingEnabled) {
			logger.info("#Instagram : Retrieving User Feed : " + uName);
		}
		
		List<UserFeedData>revUsers = null; 
		try {
			UserFeed userf = instagram.searchUser(uName);
			revUsers = userf.getUserList();
		}
		catch(InstagramException e) {
			logger.error("#Instagram Exception : " + e.getMessage());
			return response;
		}
		
		for(UserFeedData revUser : revUsers) {

			try {
				try {
					mediaFeed = instagram.getRecentMediaFeed(revUser.getId(), 0, null, null, null, null);
				}
				catch(InstagramException e) {
					logger.error("#Instagram Exception:" + e.getMessage());
					break;
				} 
				
				if(mediaFeed != null) {
					
					for(MediaFeedData mfeed : mediaFeed.getData()) {
						int createdTime = Integer.parseInt(mfeed.getCreatedTime());
						Date publicationDate = new Date((long) createdTime * 1000);
						
						if(lastItemDate.after(publicationDate) ||  numberOfRequests>maxRequests) {
							break;
    					}
						
						if(mfeed != null && mfeed.getId() != null) {
							InstagramPost instagramItem = new InstagramPost(mfeed);
							//instagramItem.setList(label);
								
							posts.add(instagramItem);
						}
					}
				}
				
			}
			catch (MalformedURLException e) {
				logger.error("#Instagram Exception: " + e.getMessage());
				break;
			}
		}
		
		// The next request will retrieve only items of the last day
		Date dateToRetrieve = new Date(System.currentTimeMillis() - (24*3600*1000));
		feed.setSinceDate(dateToRetrieve);
		
		if(loggingEnabled) {
			logger.info("#Instagram : Handler fetched " + posts.size() + " photos from " + uName + 
					" [ " + lastItemDate + " - " + new Date(System.currentTimeMillis()) + " ]");
		}
		
		response.setRequests(numberOfRequests);
		response.setPosts(posts);
		return response;
	}
	
	@Override
	public Response retrieveKeywordsFeed(KeywordsFeed feed, Integer maxRequests) {
		
		Response response = new Response();
		List<Post> posts = new ArrayList<Post>();
		
		Date lastItemDate = feed.getSinceDate();
		String label = feed.getLabel();
		
		boolean isFinished = false;
		
		int numberOfRequests = 0;

		List<String> keywords = feed.getKeywords();
		
		if(keywords == null || keywords.isEmpty()){
			logger.error("#Instagram : No keywords feed");
			return response;
		}
		
		String tags = "";
		for(String key : keywords) {
			String [] words = key.split(" ");
			for(String word : words) {
				if(!tags.contains(word) && word.length()>1) {
					tags += word.toLowerCase();
				}
			}
		}
		
		tags = tags.replaceAll(" ", "");
	
		if(tags.equals(""))
			return response;
		
		//retrieve first page
		try {
			tagFeed = instagram.getRecentMediaTags(tags);
			numberOfRequests++;
		}
		catch(InstagramException e) {	
			return response;
		}
		
		Pagination pagination = tagFeed.getPagination();
		if(tagFeed.getData() != null) {
			
			for(MediaFeedData mfeed : tagFeed.getData()) {
				int createdTime = Integer.parseInt(mfeed.getCreatedTime());
				Date publicationDate = new Date((long) createdTime * 1000);
				
				if(publicationDate.before(lastItemDate)){
					if(loggingEnabled)
						logger.info("Since date reached: " + lastItemDate);
					isFinished = true;
					break;
				}

				if(numberOfRequests > maxRequests) {
					if(loggingEnabled)
						logger.info("numberOfRequests: " + numberOfRequests + " > " + maxRequests);
					isFinished = true;
					break;
				}
				
				if(mfeed != null && mfeed.getId() != null){
					InstagramPost instagramItem;
					try {
						instagramItem = new InstagramPost(mfeed);
						//instagramItem.setList(label);
						
					} catch (MalformedURLException e) {
						logger.error("Instagram retriever exception: " + e.getMessage());
						isFinished = true;
						break;
					}
					
					posts.add(instagramItem);
					
				}
			}
				
			//continue retrieving other pages
			if(!isFinished) {
				while(pagination.hasNextPage()) {
					
					try {
						if(numberOfRequests>=maxRequests)
							break;
						
						tagFeed = instagram.getTagMediaInfoNextPage(pagination);
						numberOfRequests++;
						pagination = tagFeed.getPagination();
						if(tagFeed.getData() != null){
							
							for(MediaFeedData mfeed : tagFeed.getData()) {
								int createdTime = Integer.parseInt(mfeed.getCreatedTime());
								Date publicationDate = new Date((long) createdTime * 1000);
								if(publicationDate.before(lastItemDate) || numberOfRequests>maxRequests) {
									isFinished = true;
									break;
								}
								
								if(mfeed != null && mfeed.getId() != null){
									InstagramPost instagramItem = new InstagramPost(mfeed);
									//instagramItem.setList(label);
									
									posts.add(instagramItem);
								}
	
							}
							if(isFinished)
								break;
						}
					}
					catch(InstagramException e) {	
						logger.error("#Second Instagram Exception: " + e.getMessage());
						isFinished = true;
						break;
					} catch (MalformedURLException e1) {
						isFinished = true;
						break;
					}

				}
			}
			
		}

		if(loggingEnabled) {
			logger.info("#Instagram : Handler fetched " + posts.size() + " posts from " + tags + 
					" [ " + lastItemDate + " - " + new Date(System.currentTimeMillis()) + " ]");
		}
		
		// The next request will retrieve only items of the last day
		Date dateToRetrieve = new Date(System.currentTimeMillis() - (24*3600*1000));
		feed.setSinceDate(dateToRetrieve);
		
		response.setPosts(posts);
		response.setRequests(numberOfRequests);
		return response;
	}
	
	/*
	public List<Item> retrieveLocationFeed(LocationFeed feed, Integer maxRequests, Integer maxResults) {
		List<Item> items = new ArrayList<Item>();
		
		Date lastItemDate = feed.getDateToRetrieve();
		Date currentDate = new Date(System.currentTimeMillis());
		DateUtil dateUtil = new DateUtil();
		
		int it = 0 ;
		int numberOfRequests = 0;
		
		boolean isFinished = false;
		
		Location loc = feed.getLocation();
		
    	if(loc == null){ 
    		logger.error("#Instagram : No Location feed");
    		return items;
    	}
		
		List<org.jinstagram.entity.common.Location> locations = null;
		
    	double latitude = loc.getLatitude();
    	double longtitude = loc.getLongitude();
    	
    	try{
    		LocationSearchFeed locs = instagram.searchLocation(latitude , longtitude,5000);
    		locations = locs.getLocationList();
    	}
    	catch(InstagramException e){
    		logger.error("#Instagram Exception : "+e.getMessage());
    		return items;
    	}
    	
    	for (org.jinstagram.entity.common.Location location : locations){
    		
    		Date upDate = currentDate;
    		Date downDate = dateUtil.addDays(upDate, -1);
    		
    		while(downDate.after(lastItemDate) || downDate.equals(lastItemDate)){
    	
    			it++;
    			try{
        			mediaFeed = instagram.getRecentMediaByLocation(location.getId(),0,0,upDate,downDate);
        			numberOfRequests++;
        			if(mediaFeed != null){
        				if(loggingEnabled)
        					logger.info("#Instagram : Retrieving page "+it+" that contains "+mediaFeed.getData().size()+" posts");	
            			
                		for(MediaFeedData mfeed : mediaFeed.getData()){
        					int createdTime = Integer.parseInt(mfeed.getCreatedTime());
        					Date publicationDate = new Date((long) createdTime * 1000);
        					if(lastItemDate.after(publicationDate) || items.size()>maxResults 
        							|| numberOfRequests>maxRequests){
        						isFinished = true;
								break;
        					}
        					
        					if((mfeed != null && mfeed.getId() != null)){
        						InstagramPost instagramItem = new InstagramPost(mfeed);
        						
        						items.add(instagramItem);
        					}
        					
        				}
        			}
        		}
        		catch(InstagramException e){
        			
        			return items;
        		} catch (MalformedURLException e1) {
        			return items;
					
				}
    			
    			if(isFinished)
    				break;
    				
    			upDate = downDate;
    			downDate = dateUtil.addDays(upDate, -1);
    		}
    		
    	}
    	
    	//logger.info("#Instagram : Done retrieving for this session");
		//logger.info("#Instagram : Handler fetched " + totalRetrievedItems + " posts from (" + latitude+","+longtitude+")" + 
		//		" [ " + lastItemDate + " - " + new Date(System.currentTimeMillis()) + " ]");
		
		// The next request will retrieve only items of the last day
		Date dateToRetrieve = new Date(System.currentTimeMillis() - (24*3600*1000));
		feed.setDateToRetrieve(dateToRetrieve);
		
    	return items;
    }
	*/
	
	@Override
	public Response retrieveGroupFeed(GroupFeed feed, Integer maxRequests) {
		return new Response();
	}

	/*
	public MediaItem getMediaItem(String shortId) {
		try {
			String id = getMediaId("http://instagram.com/p/"+shortId);
			if(id == null)
				return null;
			
			MediaInfoFeed mediaInfo = instagram.getMediaInfo(id);
			if(mediaInfo != null) {
				MediaFeedData mediaData = mediaInfo.getData();
				Images images = mediaData.getImages();
				
				ImageData standardUrl = images.getStandardResolution();
				String url = standardUrl.getImageUrl();
				
				MediaItem mediaItem = new MediaItem(new URL(url));
				
				ImageData thumb = images.getThumbnail();
				String thumbnail = thumb.getImageUrl();
				
				String mediaId = "Instagram#" + mediaData.getId();
				List<String> tags = mediaData.getTags();
				
				String title = null;
				Caption caption = mediaData.getCaption();
				if(caption !=  null) {
					title = caption.getText();
				}
				
				Long publicationTime = new Long(1000*Long.parseLong(mediaData.getCreatedTime()));
				
				//id
				mediaItem.setId(mediaId);
				//SocialNetwork Name
				mediaItem.setSource("Instagram");
				//Reference
				mediaItem.setRef(id);
				//Type 
				mediaItem.setType("image");
				//Time of publication
				mediaItem.setPublicationTime(publicationTime);
				//PageUrl
				mediaItem.setPageUrl(url);
				//Thumbnail
				mediaItem.setThumbnail(thumbnail);
				//Title
				mediaItem.setTitle(title);
				//Tags
				mediaItem.setTags(tags.toArray(new String[tags.size()]));
				//Popularity
				mediaItem.setLikes(new Long(mediaData.getLikes().getCount()));
				mediaItem.setComments(new Long(mediaData.getComments().getCount()));
				//Location
				org.jinstagram.entity.common.Location geoLocation = mediaData.getLocation();
				if(geoLocation != null) {
					double latitude = geoLocation.getLatitude();
					double longitude = geoLocation.getLongitude();
					
					Location location = new Location(latitude, longitude);
					location.setName(geoLocation.getName());
					mediaItem.setLocation(location);
				}
				//Size
				ImageData standard = images.getStandardResolution();
				if(standard!=null) {
					int height = standard.getImageHeight();
					int width = standard.getImageWidth();
					mediaItem.setSize(width, height);
				}
				
				User user = mediaData.getUser();
				if(user != null) {
					StreamUser streamUser = new InstagramAccount(user);
					mediaItem.setUser(streamUser);
					mediaItem.setUserId(streamUser.getId());
				}
				
				return mediaItem;
			}
		} catch (Exception e) {
			logger.error(e);
		} 
		
		return null;
	}
	*/
	
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
