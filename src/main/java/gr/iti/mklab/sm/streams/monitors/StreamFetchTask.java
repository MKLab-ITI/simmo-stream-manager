package gr.iti.mklab.sm.streams.monitors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import gr.iti.mklab.sm.feeds.Feed;
import gr.iti.mklab.sm.retrievers.Response;

import org.apache.log4j.Logger;

import gr.iti.mklab.sm.streams.Stream;


/**
 * Class for handling a Stream Task that is responsible for retrieving
 * content for the stream it is assigned to.
 * 
 * @author Manos Schinas
 * @email  manosetro@iti.gr
 */
public class StreamFetchTask implements Runnable {
	
	private final Logger logger = Logger.getLogger(StreamFetchTask.class);
	
	private Stream stream;
	
	private Map<String, FeedFetch> feeds = Collections.synchronizedMap(new HashMap<String, FeedFetch>());
	private LinkedBlockingQueue<Feed> feedsQueue = new LinkedBlockingQueue<Feed>();
	
	private int maxRequests;
	private long period;
	
	private AtomicInteger requests = new AtomicInteger(0);
	private long lastResetTime = 0l;
	
	private long lastExecutionTime = 0l;
	private String lastExecutionFeed = null;
	
	private long totalRetrievedItems = 0;
	
	public StreamFetchTask(Stream stream) throws Exception {
		this.stream = stream;
		
		this.maxRequests = stream.getMaxRequests();
		this.period = stream.getTimeWindow() * 60000;
		
	}
	
	/**
	 * Adds the input feeds to search for relevant content in the stream
	 * 
	 * @param Feed feed
	 */
	public void addFeed(Feed feed) {
		FeedFetch feedFetch = new FeedFetch(feed);
		this.feeds.put(feed.getId(), feedFetch);
		this.feedsQueue.offer(feed);
	}
	
	/**
	 * Adds the input feeds to search for relevant content in the stream
	 * 
	 * @param List<Feed> feeds
	 */
	public void addFeeds(List<Feed> feeds) {
		for(Feed feed : feeds) {
			addFeed(feed);
		}
	}
	
	/**
	 * Remove input feed from task
	 * 
	 * @param Feed feed
	 */
	public void removeFeed(Feed feed) {
		this.feeds.remove(feed.getId());
		this.feedsQueue.remove(feed);
	}

	/**
	 * Remove input feed from task
	 * 
	 * @param Feed feed
	 */
	public void removeFeeds(List<Feed> feeds) {
		for(Feed feed : feeds) {
			removeFeed(feed);
		}
	}
	
	public List<Feed> getFeedsToPoll() {
		List<Feed> feedsToPoll = new ArrayList<Feed>();
		
		long currentTime = System.currentTimeMillis();
		// Check for new feeds
		for(FeedFetch feedFetch : feeds.values()) {
			// each feed can run one time in each period
			if((currentTime - feedFetch.getLastExecution()) > period) { 
				feedsToPoll.add(feedFetch.getFeed());
			}
		}
		return feedsToPoll;
	}

	@Override
	public void run() {
		int maxRequestsPerFeed = (int) (0.2 * maxRequests);
		
		while(true) {
			try {
				long currentTime = System.currentTimeMillis();
				if((currentTime -  lastResetTime) > period) {
					logger.info((maxRequests - requests.get()) + " available requests for " + stream.getName() + ". Reset them to " + maxRequests);
					
					requests.set(0);	// reset performed requests
					lastResetTime = currentTime;
				}

				// get feeds ready for polling
				Feed feed = feedsQueue.take();
				feedsQueue.offer(feed);
				
				List<Feed> feedsToPoll = getFeedsToPoll();
				if(!feedsToPoll.isEmpty()) {
					if(feedsToPoll.contains(feed)) {
						int requestsPerFeed = Math.min(maxRequestsPerFeed, (maxRequests - requests.get()) / feedsToPoll.size());
						if(requestsPerFeed < 1) {
							logger.info("No more remaining requests for " + stream.getName());
							logger.info("Wait for " + (currentTime -  lastResetTime - period)/1000 + " seconds until reseting.");
						}
						else {
							logger.info("Poll for [" + feed.getId() + "]. Requests: " + requestsPerFeed);
							
							Response response = stream.poll(feed, requestsPerFeed);
							totalRetrievedItems += response.getNumberOfPosts();
						
							lastExecutionTime = System.currentTimeMillis();
							lastExecutionFeed = feed.getId();
							
							// increment performed requests
							requests.addAndGet(response.getRequests());
							
							FeedFetch feedFetch = feeds.get(feed.getId());
							if(feedFetch != null) {
								feedFetch.setLastExecution(lastExecutionTime);
								feedFetch.incFetchedItems(response.getNumberOfPosts());
							}
							else {
								logger.error("There is no fetch structure for feed (" + feed.getId() + ")");
							}

						}
					}
				}
				else {
					Thread.sleep(2000);
				}				
			} catch (Exception e) {
				logger.error("Exception in stream fetch task for " + stream.getName(), e);
			}	
		}
	}
	
	public long getLastExecutionTime() {
		return lastExecutionTime;
	}

	public void setLastExecutionTime(long lastExecutionTime) {
		this.lastExecutionTime = lastExecutionTime;
	}

	public String getLastExecutionFeed() {
		return lastExecutionFeed;
	}

	public void setLastExecutionFeed(String lastExecutionFeed) {
		this.lastExecutionFeed = lastExecutionFeed;
	}

	public long getTotalRetrievedItems() {
		return totalRetrievedItems;
	}

	public void setTotalRetrievedItems(long totalRetrievedItems) {
		this.totalRetrievedItems = totalRetrievedItems;
	}

	public class FeedFetch {
		
		private Feed feed;
		private Long lastExecution = 0L;
		
		private Long fetchedItems = 0L;
		
		public FeedFetch(Feed feed) {
			this.feed = feed;
		}

		public Long getLastExecution() {
			return lastExecution;
		}

		public void setLastExecution(Long lastExecution) {
			this.lastExecution = lastExecution;
		}

		public Feed getFeed() {
			return feed;
		}

		public void setFeed(Feed feed) {
			this.feed = feed;
		}

		public Long getFetchedItems() {
			return fetchedItems;
		}

		public void setFetchedItems(Long fetchedItems) {
			this.fetchedItems = fetchedItems;
		}
		
		public void incFetchedItems(Integer fetchedItems) {
			this.fetchedItems += fetchedItems;
		}
	}
}
