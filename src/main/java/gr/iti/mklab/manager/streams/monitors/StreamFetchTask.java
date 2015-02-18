package gr.iti.mklab.sfc.streams.monitors;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.feeds.Feed;
import gr.iti.mklab.sfc.streams.Stream;


/**
 * Class for handling a Stream Task that is responsible for retrieving
 * content for the stream it is assigned to.
 * 
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class StreamFetchTask implements  Callable<List<Item>> {
	
	private final Logger logger = Logger.getLogger(StreamFetchTask.class);
	
	private Stream stream;
	
	private List<Feed> feeds = new ArrayList<Feed>();
	
	public StreamFetchTask(Stream stream) throws Exception {
		this.stream = stream;
		if(!this.stream.setMonitor()) {
			throw new Exception("Feeds monitor for stream: " + this.stream.getClass() + " cannot be initialized");
		}
	}
	
	public StreamFetchTask(Stream stream, List<Feed> feeds) throws Exception {
		this.stream = stream;
		this.feeds.addAll(feeds);
		
		if(!this.stream.setMonitor()) {
			throw new Exception("Feeds monitor for stream: " + this.stream.getClass() + " cannot be initialized");
		}
	}
	
	/**
	 * Adds the input feeds to search 
	 * for relevant content in the stream
	 * @param feeds
	 */
	public void addFeeds(List<Feed> feeds) {
		this.feeds.addAll(feeds);
	}

	/**
	 * Retrieves content using the feeds assigned to the task
	 * making rest calls to stream's API. 
	 */
	@Override
	public List<Item> call() throws Exception {
		try {
			List<Item> items = stream.poll(feeds);
			
			return items;
		} catch (Exception e) {
			logger.error("ERROR IN STREAM FETCH TASK: " + e.getMessage());
		}
		
		return new ArrayList<Item>();
	}
}
