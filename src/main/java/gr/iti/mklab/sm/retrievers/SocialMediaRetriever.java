package gr.iti.mklab.sm.retrievers;

import gr.iti.mklab.simmo.core.UserAccount;
import gr.iti.mklab.sm.Credentials;
import gr.iti.mklab.sm.feeds.AccountFeed;
import gr.iti.mklab.sm.feeds.Feed;
import gr.iti.mklab.sm.feeds.GroupFeed;
import gr.iti.mklab.sm.feeds.KeywordsFeed;

/**
 * The interface for retrieving from social media - Currently the
 * social networks supprorted by the platform are the following:
 * YouTube, Google+,Twitter, Facebook, Flickr, Instagram, Topsy, 
 * Tumblr, Vimeo, DailyMotion, Twitpic
 * 
 * @author Manos Schinas
 */
public abstract class SocialMediaRetriever implements Retriever {

	public SocialMediaRetriever(Credentials credentials) {
		
	}
	
	/**
	 * Retrieves a keywords feed that contains certain keywords
	 * in order to retrieve relevant content
	 * 
	 * @param feed - A KeywordsFeed
	 *  
	 * @return gr.iti.mklab.framework.retrievers.Response
	 * @throws Exception during any retrieval errors
	 */
	public Response retrieveKeywordsFeed(KeywordsFeed feed) throws Exception {
		return retrieveKeywordsFeed(feed, 1);
	}
	
	public abstract Response retrieveKeywordsFeed(KeywordsFeed feed, Integer maxRequests) throws Exception;
	
	/**
	 * 	Retrieves a user feed that contains the user/users in 
	 * 	order to retrieve content posted by them
	 * 
	 *  @param feed - An AccountFeed
	 * 
	 *	@return gr.iti.mklab.framework.retrievers.Response
	 *	@throws Exception during any retrieval errors
	 */
	public Response retrieveAccountFeed(AccountFeed feed) throws Exception {
		return retrieveAccountFeed(feed, 1);
	}
	
	public abstract Response retrieveAccountFeed(AccountFeed feed, Integer maxRequests) throws Exception;
	
	/**
	 * Retrieves a list feed that contains the owner of a list an a slug 
	 * used for the description of the list.
	 * 
	 * @param feed - A GroupFeed
	 * 
	 * @return gr.iti.mklab.framework.retrievers.Response
	 */
	public Response retrieveGroupFeed(GroupFeed feed) {
		return retrieveGroupFeed(feed, 1);
	}
	
	public abstract Response retrieveGroupFeed(GroupFeed feed, Integer maxRequests);
	
	
	/**
	 * Retrieves the info for a specific user on the basis
	 * of his id in the social network
	 * 
	 * @param uid User id
	 * 
	 * @return gr.iti.mklab.simmo.UserAccount
	 */
	public abstract UserAccount getStreamUser(String uid);
	
	@Override
	public Response retrieve(Feed feed) throws Exception {
		return retrieve(feed, 1);
	}
	
	@Override
	public Response retrieve (Feed feed, Integer requests) throws Exception {
	
		if(AccountFeed.class.isInstance(feed)) {
			AccountFeed userFeed = (AccountFeed) feed;				
			return retrieveAccountFeed(userFeed, requests);
		}
		if(KeywordsFeed.class.isInstance(feed)) {
			KeywordsFeed keyFeed = (KeywordsFeed) feed;				
			return retrieveKeywordsFeed(keyFeed, requests);
		}
		if(GroupFeed.class.isInstance(feed)) {
			GroupFeed listFeed = (GroupFeed) feed;
			return retrieveGroupFeed(listFeed, requests);
		}
		
		
		return new Response();
	}
	
}
