package gr.iti.mklab.sm.streams.monitors;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import gr.iti.mklab.sm.feeds.Feed;

import org.apache.log4j.Logger;

import gr.iti.mklab.sm.streams.Stream;


/**
 * Thread-safe class for monitoring the streams that correspond to each social network
 * Currently 7 social networks are supported (Twitter, Youtube, Flickr, Instagram, Tumblr, Facebook, GooglePlus)
 * 
 * @author Manos Schinas
 * @email  manosetro@iti.gr
 */
public class StreamsMonitor implements Runnable {
	
	public final Logger logger = Logger.getLogger(StreamsMonitor.class);

	private ExecutorService executor;
	
	private Map<String, Stream> streams = new HashMap<String, Stream>();
	private Map<String, StreamFetchTask> streamsFetchTasks = new HashMap<String, StreamFetchTask>();
	
	boolean isFinished = false;

	private Future<?> monitorFuture;

	public StreamsMonitor(int numberOfStreams) {
		logger.info("Initialize Execution Service with " + numberOfStreams + " threads.");
		executor = Executors.newFixedThreadPool(numberOfStreams + 1);
	}
	
	public int getNumberOfStreamFetchTasks() {
		return streamsFetchTasks.size();
	}
	
	/**
	 * Adds the streams to the monitor
	 * @param streams
	 */
	public void addStreams(List<Stream> streams) {
		for(Stream stream : streams) {
			addStream(stream);
		}
	}

	/**
	 * Add one stream to the monitor mapped to its id
	 * @param streamId
	 * @param stream
	 */
	public void addStream(Stream stream) {
		
		String streamId = stream.getName(); 
		this.streams.put(streamId, stream);
		
		try {
			logger.info("Start " + streamId + " Fetch Task");
			StreamFetchTask streamTask = new StreamFetchTask(stream);
			streamsFetchTasks.put(streamId, streamTask);
		} catch (Exception e) {
			logger.error(e);
		}
		
	}
	
	public Stream getStream(String streamId) {
		return streams.get(streamId);
	}
	
	/**
	 * Adds a feed to the monitor
	 * @param stream
	 */
	public void addFeed(String streamId, Feed feed) {
		StreamFetchTask fetchTask = streamsFetchTasks.get(streamId);
		if(fetchTask != null) {
			fetchTask.addFeed(feed);
		}
	}
	
	/**
	 * Adds a feed to the monitor
	 * @param stream
	 */
	public void removeFeed(String streamId, Feed feed) {
		StreamFetchTask fetchTask = streamsFetchTasks.get(streamId);
		if(fetchTask != null) {
			fetchTask.removeFeed(feed);
		}
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
	 * Starts the retrieval process for all streams. Each stream is served
	 * by a different thread->StreamFetchTask
	 * @param 
	 */
	public void start() {
		monitorFuture = executor.submit(this);
	}
	
	/**
	 * Stops the monitor - waits for all streams to shutdown
	 */
	public void stop() {
		isFinished = true;
		monitorFuture.cancel(true);
		
		for(String streamId : streamsFetchTasks.keySet()) {
			StreamFetchTask fetchTask = streamsFetchTasks.get(streamId);
			if(fetchTask != null) {
				fetchTask.stop();		
			}
		}
		
        // Don't do this gracefully with shutdown(). Often it never really shuts down
		executor.shutdownNow();
		/*
        while (!executor.isTerminated()) {
        	try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				logger.error(e);
			}
        	logger.info("Waiting for StreamsMonitor to shutdown");
        }
        */
        logger.info("Streams Monitor stopped");
	}

	@Override
	public void run() {
		for(String streamId : streamsFetchTasks.keySet()) {
			StreamFetchTask task = streamsFetchTasks.get(streamId);
			logger.info("Submit fetch task for " + streamId + " for execution.");
			Future<?> future = executor.submit(task);
			task.setFuture(future);
		}
		
		while(!isFinished) {
			// print statistics every 30 seconds
			for(String streamId : streamsFetchTasks.keySet()) {
				StreamFetchTask task = streamsFetchTasks.get(streamId);
				if(task != null) {
					logger.info("Fetch task for " + streamId + " has fetched " + task.getTotalRetrievedItems() + " items in total");
					logger.info("Last execution time for " + streamId + ": " + task.getLastExecutionTime() + " for feed (" + task.getLastExecutionFeed() + ")");
				}
			}
			
			try {
				Thread.sleep(300000);
			} catch (InterruptedException e) {
				logger.error(e);
				return;
			}
		}
	}
	
	public Map<String, Object> getStatus() {
		
		Map<String, Object> stats = new HashMap<String, Object>();
		
		for(String sId : streams.keySet()) {
			
			StreamFetchTask sft = streamsFetchTasks.get(sId);
			
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("numberOfFeeds", sft.numberOfFeeds());
			map.put("retrievedItems", sft.getTotalRetrievedItems());
			map.put("lastExecutionTime", new Date(sft.getLastExecutionTime()));
			
			List<String> feedIds = new ArrayList<String>();
			for(Feed feed : sft.getFeeds()) {
				feedIds.add(feed.getId());
			}
			map.put("feeds", feedIds);
			
			stats.put(sId, map);
		}
		
		
		return stats;
	}
	
	/*
	@Override
	public void run() {
		Map<String, Future<Integer>> responses = new HashMap<String, Future<Integer>>();
		while(!isFinished) {
			for(String streamId : streamsFetchTasks.keySet()) {
				StreamFetchTask task = streamsFetchTasks.get(streamId);
				
				List<Feed> feeds = task.getFeedsToPoll();
				if(!feeds.isEmpty()) {
					Future<Integer> response = responses.get(streamId);
					if(response == null || response.isDone()) {
						logger.info("Submit new task in " + streamId);
						Future<Integer> futureResponse = executor.submit(task);
						responses.put(streamId, futureResponse);
					}
					else {
						//logger.info(streamId + " is running");
					}
				}
			}
			
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				return;
			}
		}
	}
	*/
	
}