package gr.iti.mklab.manager.streams;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.feeds.Feed;
import gr.iti.mklab.framework.retrievers.Retriever;
import gr.iti.mklab.simmo.documents.Post;


/**
 * Class responsible for handling the stream of information regarding 
 * a social network or a news feed source.
 * 
 * It is responsible for the configuration of the connection to the selected API
 * and the retrieval/storing of relevant content.
 * @author manosetro
 * @email  manosetro@iti.gr
 * @author ailiakop
 * @email  ailiakop@iti.gr
 *
 */
public abstract class Stream {//implements Runnable {

	protected static final String KEY = "Key";
	protected static final String SECRET = "Secret";
	protected static final String ACCESS_TOKEN = "AccessToken";
	protected static final String ACCESS_TOKEN_SECRET = "AccessTokenSecret";
	protected static final String CLIENT_ID = "ClientId";
	protected static final String APP_ID = "AppId";
	protected static final String APP_SECRET = "AppSecret";
	
	protected static final String MAX_RESULTS = "maxResults";
	protected static final String MAX_REQUESTS = "maxRequests";
	protected static final String MAX_RUNNING_TIME = "maxRunningTime";
	
	protected FeedsMonitor monitor;
	
	protected BlockingQueue<Feed> feedsQueue;
	protected Retriever retriever = null;
	
	protected StorageHandler handler;
	
	private Logger  logger = Logger.getLogger(Stream.class);
	
	/**
	 * Opens a stream for updates delivery
	 * @param config
	 *      Stream configuration parameters
	 * @throws StreamException
	 *      In any case of error during stream open
	 */
	public abstract void open(StreamConfiguration config) throws Exception;
	
	/**
	 * Closes a stream 
	 * @throws StreamException
	 *      In any case of error during stream close
	 */
	public void close() throws Exception {
	
		if(monitor != null) {
			logger.info("Stop monitor");
			monitor.stopMonitor();
		}
		
		if(retriever != null) {
			logger.info("Stop retriever");
			retriever.stop();
		}
		
		logger.info("Close Stream  : " + this.getClass().getName());
	}
		
	/**
	 * Sets the handler that is responsible for the handling 
	 * of the retrieved items
	 * @param handler
	 */
	public void setHandler(StorageHandler handler) {
		this.handler = handler;
	}
	
	/**
	 * Sets the feeds monitor for the stream
	 * @return
	 */
	public boolean setMonitor() {
		if(retriever == null)
			return false;
		
		monitor = new FeedsMonitor(retriever);
		return true;
	}
	
	/**
	 * Sets the users list that will be used to retrieve from the stream (utilized for Twitter Stream)
	 * @param usersToLists
	 
	public void setUserLists(Map<String, Set<String>> usersToLists) {
		this.usersToLists = usersToLists;
		
		if(usersToLists != null) {
			Set<String> allLists = new HashSet<String>();
			for(Set<String> lists : usersToLists.values()) {
				allLists.addAll(lists);
			}
			logger.info("=============================================");
			logger.info(usersToLists.size() + " user in " + allLists.size() + " Lists!!!");
		}
	}
	*/
	
	/**
	 * Returns the list of retrieved items 
	 * @return
	 */
//	public synchronized List<Item> getRetrievedItems() {
//		return retrievedItems;
//	}
	
	/**
	 * Searches with the wrapper of the stream for a particular
	 * set of feeds (feeds can be keywordsFeeds, userFeeds, locationFeeds, listFeeds or URLFeeds)
	 * @param feeds
	 * @throws StreamException
	 */
	public synchronized List<Post> poll(List<Feed> feeds) throws Exception {
		List<Post> retrievedItems = new ArrayList<Post>();
		if(retriever != null) {
		
			if(feeds == null) {
				logger.error("Feeds is null in poll method.");
				return retrievedItems;
			}
			
			//logger.info(getName() + ": poll for " + feeds.size() + " feeds");
			for(Feed feed : feeds) {
				try {
					List<Post> items = retriever.retrieve(feed);
					store(items);
					retrievedItems.addAll(items);
				}
				catch(Exception e) {
					logger.error("Exception for feed " + feed.getId() + " of type " + feed.getFeedtype() + " from "
							+ getName());
					logger.error("Feed: " + feed.toString());
					logger.error(e.getMessage());
				}
			}
			
			//logger.info("Retrieved items for " + getName() + " are : " + retrievedItems.size());
		}
		return retrievedItems;
		
	}
	
	/**
	 * Searches with the wrapper of the stream for a particular
	 * feed (feed can be keywordsFeeds, userFeeds, locationFeeds, listFeeds or URLFeeds)
	 * @param feeds
	 * @throws StreamException
	 */
	public synchronized List<Post> poll(Feed feed) throws Exception {
		List<Post> retrievedItems = new ArrayList<Post>();
		if(retriever != null) {
		
			if(feed == null) {
				logger.error("Feeds is null in poll method.");
				return retrievedItems;
			}
			
			try {
				List<Post> items = retriever.retrieve(feed);
				store(items);
				retrievedItems.addAll(items);
			}
			catch(Exception e) {
				logger.error("Exception for feed " + feed.getId() + " of type " + feed.getFeedtype());
				logger.error(e.getMessage());
			}
			
			//logger.info("Retrieved items for " + getName() + " are : " + retrievedItems.size());
		}
		return retrievedItems;
	}
	
	/**
	 * Store a set of items in the selected databases
	 * @param items
	 */
	public synchronized void store(List<Post> items) {
		for(Post item : items) {
			store(item);
		}
	}
	
	/**
	 * Store an item in the selected databases
	 * @param item
	 */
	public synchronized void store(Post item) {
		
		if(handler == null) {
			logger.error("NULL Handler!");
			return;
		}
		
		handler.update(item);
	}
	
	/**
	 * Deletes an item from the selected databases
	 * @param item
	 */
	public void delete(Post item) {
		handler.delete(item);
	}
	
	/**
	 * Adds a feed to the stream for future searching
	 * @param feed
	 * @return
	 */
	public boolean addFeed(Feed feed) {
		if(feedsQueue == null) {
			return false;
		}
		
		return feedsQueue.offer(feed);
	}
	/**
	 * Adds a set of feeds to the stream for future searching
	 * @param feeds
	 * @return
	 */
	public boolean addFeeds(List<Feed> feeds) {
		for(Feed feed : feeds) {
			if(!addFeed(feed)) {
				return false;
			}
		}
		return true;
	}
	
	public abstract String getName();
	
}
