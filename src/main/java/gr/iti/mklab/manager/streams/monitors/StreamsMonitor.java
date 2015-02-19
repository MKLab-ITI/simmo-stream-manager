package gr.iti.mklab.manager.streams.monitors;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.feeds.Feed;
import gr.iti.mklab.manager.streams.Stream;
import gr.iti.mklab.simmo.documents.Post;

/**
 * Thread-safe class for monitoring the streams that correspond to each social network
 * Currently 7 social networks are supported (Twitter, Youtube, Flickr, Instagram, Tumblr, Facebook, GooglePlus)
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 */
public class StreamsMonitor {
	
	// 30 minutes
	private static final long DEFAULT_REQUEST_PERIOD = 1 * 30 * 60000; 
	
	public final Logger logger = Logger.getLogger(StreamsMonitor.class);

	private ExecutorService executor;
	
	private Map<String, Stream> streams = new HashMap<String, Stream>();
	private Map<String, List<Feed>> feedsPerStream = new HashMap<String, List<Feed>>();
	
	private Map<String, Long> runningTimePerStream = new HashMap<String, Long>();
	
	private Map<String, StreamFetchTask> streamsFetchTasks = new HashMap<String, StreamFetchTask>();
	private Map<String, Future<List<Post>>> responses = new HashMap<String, Future<List<Post>>>();
	
	boolean isFinished = false;
	
	private ReInitializer reInitializer;
	
	public StreamsMonitor(int numberOfStreams) {
		logger.info("Initialize Execution Searvice with " + numberOfStreams + " threads.");
		executor = Executors.newFixedThreadPool(numberOfStreams);
	}
	
	public Collection<Post> getTotalRetrievedItems() {
		Map<String, Post> totalRetrievedPosts = new HashMap<String, Post>();
		for(String streamId : responses.keySet()) {
			try {
				Future<List<Post>> response = responses.get(streamId);
				
				if(!response.isDone())
					continue;
				
				List<Post> retrievedPosts = response.get();
				if(retrievedPosts != null) {
					logger.info("Got " + retrievedPosts.size() + " posts from " + streamId);
					for(Post post : retrievedPosts) {
						totalRetrievedPosts.put(post.getId(), post);
					}
				}
			} catch (Exception e) {
				logger.error(e);
			}
		}
		return totalRetrievedPosts.values();
	}
	
	public int getNumberOfStreamFetchTasks() {
		return streamsFetchTasks.size();
	}
	
	/**
	 * Adds the streams to the monitor
	 * @param streams
	 */
	public void addStreams(Map<String, Stream> streams) {
		for(String streamId : streams.keySet()) {
			addStream(streamId, streams.get(streamId));
		}
	}

	/**
	 * Add one stream to the monitor mapped to its id
	 * @param streamId
	 * @param stream
	 */
	public void addStream(String streamId, Stream stream) {
		this.streams.put(streamId, stream);
	}
	
	/**
	 * Adds a stream to the monitor mapped to its id and the feeds that
	 * will be used to retrieve relevant content from the aforementioned
	 * stream. Request time refers to the time period the stream will 
	 * serve search requests. 
	 * @param streamId
	 * @param stream
	 * @param feeds
	 */
	public void addStream(String streamId, Stream stream, List<Feed> feeds) {
		this.streams.put(streamId, stream);
		this.feedsPerStream.put(streamId, feeds);
	}
	
	public Stream getStream(String streamId) {
		return streams.get(streamId);
	}
	
	/**
	 * Adds a feed to the monitor
	 * @param stream
	 */
	public void addFeeds(String streamId, List<Feed> feeds) {
		StreamFetchTask fetchTask = streamsFetchTasks.get(streamId);
		if(fetchTask != null) {
			fetchTask.addFeeds(feeds);
		}
	}
	
	/**
	 * Adds a feed to the monitor
	 * @param stream
	 */
	public void addFeeds(List<Feed> feeds) {
		for(StreamFetchTask fetchTask : streamsFetchTasks.values()) {
			fetchTask.addFeeds(feeds);
		}
	}
	
	/**
	 * Starts searching into the specific stream by assigning its feeds to stream fetch tasks and
	 * executing them.
	 * @param streamId
	 */
	public void startStream(String streamId) {
		if(!streams.containsKey(streamId)) {
			logger.error("Stream "+streamId+" needs to be added to the monitor first");
			return;
		}
		
		try {
			List<Feed> feeds = feedsPerStream.get(streamId);
			if(feeds==null || feeds.isEmpty())
				return;
			
			StreamFetchTask streamTask = new StreamFetchTask(streams.get(streamId), feeds);
			
			streamsFetchTasks.put(streamId, streamTask);
			
			Future<List<Post>> response = executor.submit(streamTask);
			responses.put(streamId, response);
	
			runningTimePerStream.put(streamId, System.currentTimeMillis());
			
			logger.info("Start stream task: " + streamId + " with " + feedsPerStream.get(streamId).size() + " feeds");
			
		} catch (Exception e) {
			logger.error(e);
		}
	}

	/**
	 * Starts the retrieval process for all streams. Each stream is served
	 * by a different thread->StreamFetchTask
	 * @param 
	 */
	public void startStreams() {
		for(String streamId : streams.keySet()) {
			startStream(streamId);
		}
		reInitializer = new ReInitializer();
		reInitializer.start();
	}
	
	public void startReInitializer() {
		reInitializer = new ReInitializer();
		reInitializer.start();
	}
	
	/**
	 *	Starts the retrieval process for all the streams added. Each stream is served by a different thread -> StreamFetchTask. 
	 *	All streams are assigned the same input feeds for retrieving relevant content.
	 * 	@param feeds
	 * 	@throws Exception 
	 */
	public void retrieve(List<Feed> feeds) throws Exception {
		for(Map.Entry<String, Stream> entry : streams.entrySet()) {
			String streamId = entry.getKey();
			try {
				Stream stream = entry.getValue();
				StreamFetchTask fetchTask = new StreamFetchTask(stream, feeds);
				Future<List<Post>> response = executor.submit(fetchTask);
				responses.put(streamId, response);
			}
			catch(Exception e) {
				e.printStackTrace();
				logger.error(e.getMessage());
			}
		}
	}
	
	/**
	 * Starts the retrieval process for all the streams specified here with a 
	 * standard list of input feeds to retrieve relevant content. Each stream is served
	 * by a different thread->StreamFetchTask.
	 * @param selectedStreams
	 * @param feeds
	 */
	public void retrieve(Set<String> streamIds, List<Feed> feeds) {
		for(Map.Entry<String, Stream> entry : streams.entrySet()) {
			String streamId = entry.getKey();
			if(streamIds.contains(streamId)) {
				try {
					Stream stream = entry.getValue();
					StreamFetchTask fetchTask = new StreamFetchTask(stream, feeds);
					Future<List<Post>> response = executor.submit(fetchTask);
					responses.put(streamId, response);
					logger.info("Start stream task : " + entry.getKey() + " with " + feeds.size() + " feeds");
				} catch (Exception e) {
					logger.error(e);
				}
			}
		}
	}
	
	/**
	 * Restarts a stream to start retrieving again for relevant content to its input feeds. 
	 * Reinitializer checks the last time the stream was searched and if the specified time 
	 * period has passed, restarts the stream. 
	 * @author ailiakop
	 *
	 */
	private class ReInitializer extends Thread {
			
		public ReInitializer() {
			logger.info("ReInitializer Thread instantiated");
		}
		
		public void run() {
			logger.info("ReInitializer Thread started");
			while(!isFinished) {
				long currentTime = System.currentTimeMillis();
				
				for(String streamId : runningTimePerStream.keySet()) {
					if((currentTime - runningTimePerStream.get(streamId)) >= DEFAULT_REQUEST_PERIOD) {
						Future<List<Post>> response = responses.get(streamId);
						if(response == null || response.isDone() || response.isCancelled()) {	
							// Reinitialize task as the previous one is done or cancelled
							logger.info("* Re-Initialize " + streamId);
							StreamFetchTask fetchTask = streamsFetchTasks.get(streamId);				
							response = executor.submit(fetchTask);		
							responses.put(streamId, response);
							runningTimePerStream.put(streamId, System.currentTimeMillis());
						}
					}
				}
				
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					logger.error(e);
				}
			}
		}
		
	}
	
	/**
	 * Checks if all streams are finished retrieving items if so sets returns true
	 * @return
	 */
	public boolean areStreamsFinished() {		
		Set<String> streamIds = new HashSet<String>();
		streamIds.addAll(responses.keySet());
		
		for(String streamId : streamIds) {
			Future<List<Post>> response = responses.get(streamId);	
			try {
				if(response.isCancelled()) {
					responses.remove(streamId);
					logger.error(streamId + " is cancelled.");
					continue;
				}
				
				if(!response.isDone()) {
					return false;
				}
				
			} catch (Exception e) {
				logger.error(e);
				// Remove stream id from responses. 
				responses.remove(streamId);
			}
		}
		
		// All stream have finished or failed.
		return true;
	}
	
	/**
	 * Stops the monitor - waits for all streams to shutdown
	 */
	public void stop() {
		isFinished = true;
		executor.shutdown();
		
        while (!executor.isTerminated()) {
        	try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error(e);
			}
        	logger.info("Waiting for StreamsMonitor to shutdown");
        }
        logger.info("Streams Monitor stopped");
	}
	
	public void reset() {
		runningTimePerStream.clear();
		feedsPerStream.clear();
		streamsFetchTasks.clear();
		responses.clear();
	}
	
	public void status() {
		logger.info("streams: " + streams.size());
		logger.info("feedsPerStream:" + feedsPerStream.size());
		logger.info("runningTimePerStream:" + runningTimePerStream.size());
		logger.info("streamsFetchTasks:" + streamsFetchTasks.size());
	}
	
}