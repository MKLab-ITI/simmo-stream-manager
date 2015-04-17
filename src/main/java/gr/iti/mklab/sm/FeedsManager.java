package gr.iti.mklab.sm;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import gr.iti.mklab.sm.feeds.Feed;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import gr.iti.mklab.sm.input.FeedsCreator;
import gr.iti.mklab.sm.management.StorageHandler;
import gr.iti.mklab.sm.streams.Stream;
import gr.iti.mklab.sm.streams.StreamException;
import gr.iti.mklab.sm.streams.StreamsManagerConfiguration;
import gr.iti.mklab.sm.streams.monitors.StreamsMonitor;

/**
 * Class for retrieving content according to  keywords - user - location feeds from social networks.
 * Currently 7 social networks are supported (Twitter,Youtube,Facebook,Flickr,Instagram,Tumblr,GooglePlus)
 * 
 * @author Manos Schinas
 * @email  manosetro@iti.gr
 * 
 */
public class FeedsManager implements Runnable {
	
	public final Logger logger = Logger.getLogger(FeedsManager.class);
	
	enum ManagerState {
		OPEN, CLOSE
	}

	private Map<String, Stream> streams = null;
	
	private StreamsManagerConfiguration config = null;
	private StorageHandler storageHandler;
	
	private StreamsMonitor monitor;
	
	private ManagerState state = ManagerState.CLOSE;

	private FeedsCreator feedsCreator;
	private Set<Feed> feeds = new HashSet<Feed>();

	public FeedsManager(StreamsManagerConfiguration config) throws StreamException {

		if (config == null) {
			throw new StreamException("Manager's configuration must be specified");
		}
		
		//Set the configuration files
		this.config = config;
		
		//Set up the Streams
		initStreams();
	}
	
	/**
	 * Opens Manager by starting the auxiliary modules and setting up
	 * the database for reading/storing
	 * 
	 * @throws StreamException
	 */
	public synchronized void open() throws StreamException {
		
		if (state == ManagerState.OPEN) {
			return;
		}
		
		state = ManagerState.OPEN;
		logger.info("StreamsManager is open now.");
		
		try {
			//If there are Streams to monitor start the StreamsMonitor
			if(streams != null && !streams.isEmpty()) {
				monitor = new StreamsMonitor(streams.size());
			}
			else {
				throw new StreamException("There are no streams to open.");
			}
			
			//Start stream handler 
			storageHandler = new StorageHandler(config);
			storageHandler.start();	
			logger.info("Storage Manager is ready to store.");
			
			feedsCreator = new FeedsCreator(config.getInputConfig());
			
			//Start the Streams
			for (String streamId : streams.keySet()) {
				logger.info("Start Stream : " + streamId);
				Configuration sconfig = config.getStreamConfig(streamId);
				Stream stream = streams.get(streamId);
				stream.setHandler(storageHandler);
				stream.open(sconfig);
				
				monitor.addStream(stream);
			}
			monitor.start();
			
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new StreamException("Error during streams open", e);
		}
	}
	
	/**
	 * Closes Manager and its auxiliary modules
	 * 
	 * @throws StreamException
	 */
	public synchronized void close() throws StreamException {
		
		if (state == ManagerState.CLOSE) {
			logger.info("StreamManager is already closed.");
			return;
		}
		
		try {
			for (Stream stream : streams.values()) {
				logger.info("Close " + stream);
				stream.close();
			}
			
			if (storageHandler != null) {
				storageHandler.stop();
			}
			
			state = ManagerState.CLOSE;
		}
		catch(Exception e) {
			throw new StreamException("Error during streams close", e);
		}
	}
	
	/**
	 * Initializes the streams apis that are going to be searched for 
	 * relevant content
	 * @throws StreamException
	 */
	private void initStreams() throws StreamException {
		streams = new HashMap<String, Stream>();
		try {
			for (String streamId : config.getStreamIds()) {
				Configuration sconfig = config.getStreamConfig(streamId);
				
				String streamName = sconfig.getParameter(Configuration.CLASS_PATH);
				Stream stream = (Stream)Class.forName(streamName).newInstance();
				
				streams.put(streamId, stream);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new StreamException("Error during streams initialization", e);
		}
	}

	@Override
	public void run() {
		
		if(state != ManagerState.OPEN) {
			logger.error("Streams Manager is not open!");
			return;
		}
		
		while(state == ManagerState.OPEN) {
			
			Set<Feed> newFeeds = feedsCreator.createFeeds();
			
			Set<Feed> toBeRemoved = new HashSet<Feed>(feeds);
			toBeRemoved.removeAll(newFeeds);
			
			newFeeds.removeAll(feeds);
			
			feeds.addAll(newFeeds);
			feeds.removeAll(toBeRemoved);
			
			// Add/Remove feeds from monitor's list
			if(!toBeRemoved.isEmpty() || !newFeeds.isEmpty()) {				
				logger.info("Remove " + toBeRemoved.size() + " feeds");
				for(Feed feed : toBeRemoved) {
					String streamId = feed.getSource();
					if(monitor != null) {
						Stream stream = monitor.getStream(streamId);
						if(stream != null) { 
							monitor.addFeed(streamId, feed);
						}
						else {
							logger.error("Stream " + streamId + " has not initialized");
						}
					}
				}
			
				logger.info("Add " + newFeeds.size() + " new feeds");
				for(Feed feed : newFeeds) {
					String streamId = feed.getSource();
					if(monitor != null) {
						Stream stream = monitor.getStream(streamId);
						if(stream != null) { 
							monitor.addFeed(streamId, feed);
						}
						else {
							logger.error("Stream " + streamId + " has not initialized");
						}
					}
				}
			}
			
			try {
				// Check for new feeds every one minute
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				logger.error(e.getMessage());
			}
			
		}
	}
	
	public static void main(String[] args) {
		
		Logger logger = Logger.getLogger(FeedsManager.class);
		
		File streamConfigFile;
		if(args.length != 1 ) {
			streamConfigFile = new File("./conf/streams.conf.xml");
		}
		else {
			streamConfigFile = new File(args[0]);
		}
		
		FeedsManager manager = null;
		try {
			StreamsManagerConfiguration config = StreamsManagerConfiguration.readFromFile(streamConfigFile);		
	        
			
			manager = new FeedsManager(config);
			manager.open();
			
			Thread thread = new Thread(manager);
			thread.start();
			
			
		} catch (ParserConfigurationException e) {
			logger.error(e.getMessage());
		} catch (SAXException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		} catch (StreamException e) {
			logger.error(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}	
	}
}
