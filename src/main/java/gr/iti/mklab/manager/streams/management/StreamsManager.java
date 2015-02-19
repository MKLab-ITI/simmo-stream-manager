package gr.iti.mklab.manager.streams.management;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.feeds.Feed;
import gr.iti.mklab.manager.config.Configuration;
import gr.iti.mklab.manager.config.StreamManagerConfiguration;
import gr.iti.mklab.manager.input.FeedsCreator;
import gr.iti.mklab.manager.streams.Stream;

/**
 * Class for retrieving content according to 
 * keywords - user - location feeds from social networks - Currently
 * 7 social networks are supported
 * (Twitter,Youtube,Facebook,Flickr,Instagram,Tumblr,GooglePlus)
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 * 
 */
public class StreamsManager {
	
	public final Logger logger = Logger.getLogger(StreamsManager.class);
	
	enum ManagerState {
		OPEN, CLOSE
	}

	private Map<String, Stream> streams = null;
	private StreamManagerConfiguration config = null;
	
	private StorageHandler storageHandler;
	
	//private StreamsMonitor monitor;
	
	private ManagerState state = ManagerState.CLOSE;

	private List<Feed> feeds = new ArrayList<Feed>();

	public StreamsManager(StreamManagerConfiguration config) throws Exception {

		if (config == null) {
			throw new Exception("Manager's configuration must be specified");
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
	public synchronized void open() throws Exception {
		
		if (state == ManagerState.OPEN) {
			return;
		}
		
		state = ManagerState.OPEN;
		logger.info("Streams are now open");
		
		try {
			//If there are Streams to monitor start the StreamsMonitor
			if(streams != null && !streams.isEmpty()) {
				//monitor = new StreamsMonitor(streams.size());
			}
			
			//Start stream handler 
			storageHandler = new StorageHandler(config);
			storageHandler.start();	
			logger.info("Store Manager is ready to store.");
			
			FeedsCreator feedsCreator = new FeedsCreator(config);
			Map<String, List<Feed>> results = feedsCreator.createFeedsPerSource();
			
			//Start the Streams
			for (String streamId : streams.keySet()) {
				logger.info("Stream Manager - Start Stream : " + streamId);
				Configuration sconfig = config.getStreamConfig(streamId);
				Stream stream = streams.get(streamId);
				stream.setHandler(storageHandler);
				stream.open(sconfig);
				
				feeds = results.get(streamId);
				
				if(feeds == null || feeds.isEmpty()) {
					logger.error("No feeds for Stream : "+streamId);
					logger.error("Close Stream : "+streamId);
					stream.close();
					continue;
				}
				
				//if(monitor != null) {
				//	monitor.addStream(streamId, stream, feeds);
				//	monitor.startStream(streamId);
				//}
			}
			
			//if(monitor != null && monitor.getNumberOfStreamFetchTasks() > 0) {
			//	monitor.startReInitializer();
			//}

		}
		catch(Exception e) {
			e.printStackTrace();
			throw new Exception("Error during streams open", e);
		}
	}
	
	/**
	 * Closes Manager and its auxiliary modules
	 * @throws StreamException
	 */
	public synchronized void close() throws Exception {
		
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
			throw new Exception("Error during streams close", e);
		}
	}
	
	/**
	 * Initializes the streams apis that are going to be searched for 
	 * relevant content
	 * @throws StreamException
	 */
	private void initStreams() throws Exception {
		streams = new HashMap<String, Stream>();
		try{
			for (String streamId : config.getStreamIds()){
				Configuration sconfig = config.getStreamConfig(streamId);
				String className = sconfig.getParameter(Configuration.CLASS_PATH);
				streams.put(streamId,(Stream)Class.forName(className).newInstance());
			}
		}catch(Exception e) {
			e.printStackTrace();
			throw new Exception("Error during streams initialization", e);
		}
	}

}
