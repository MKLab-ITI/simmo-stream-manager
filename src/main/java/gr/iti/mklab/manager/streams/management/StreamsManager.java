package gr.iti.mklab.sfc.streams.management;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.feeds.Feed;
import gr.iti.mklab.sfc.input.DataInputType;
import gr.iti.mklab.sfc.input.FeedsCreator;
import gr.iti.mklab.sfc.input.InputConfiguration;
import gr.iti.mklab.sfc.streams.Stream;
import gr.iti.mklab.sfc.streams.StreamConfiguration;
import gr.iti.mklab.sfc.streams.StreamException;
import gr.iti.mklab.sfc.streams.StreamsManagerConfiguration;
import gr.iti.mklab.sfc.streams.monitors.StreamsMonitor;
import gr.iti.mklab.sfc.subscribers.Subscriber;

/**
 * Class for retrieving content according to 
 * keywords - user - location feeds from social networks - Currently
 * 7 social networks are supported
 * (Twitter,Youtube,Facebook,Flickr,Instagram,Tumblr,GooglePlus)
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 * 
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class StreamsManager {
	
	public final Logger logger = Logger.getLogger(StreamsManager.class);
	
	enum ManagerState {
		OPEN, CLOSE
	}

	private Map<String, Stream> streams = null;
	private Map<String, Subscriber> subscribers = null;
	private StreamsManagerConfiguration config = null;
	private InputConfiguration inputConfig = null;
	private StorageHandler storageHandler;
	private StreamsMonitor monitor;
	
	private ManagerState state = ManagerState.CLOSE;

	private List<Feed> feeds = new ArrayList<Feed>();

	public StreamsManager(StreamsManagerConfiguration config, InputConfiguration input_config) throws StreamException {

		if (config == null) {
			throw new StreamException("Manager's configuration must be specified");
		}
		
		//Set the configuration files
		this.config = config;
		this.inputConfig = input_config;
		
		//Set up the Subscribers
		initSubscribers();
		
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
		logger.info("Streams are now open");
		
		try {
			//If there are Streams to monitor start the StreamsMonitor
			if(streams != null && !streams.isEmpty()) {
				monitor = new StreamsMonitor(streams.size());
			}
			
			//Start stream handler 
			storageHandler = new StorageHandler(config);
			storageHandler.start();	
			logger.info("Store Manager is ready to store.");
			
			FeedsCreator feedsCreator = new FeedsCreator(DataInputType.MONGO_STORAGE, inputConfig);
			Map<String, List<Feed>> results = feedsCreator.getQueryPerStream();
			
			//Start the Subscribers
			for(String subscriberId : subscribers.keySet()) {
				logger.info("Stream Manager - Start Subscriber : " + subscriberId);
				StreamConfiguration srconfig = config.getSubscriberConfig(subscriberId);
				Subscriber subscriber = subscribers.get(subscriberId);
				subscriber.setHandler(storageHandler);
				subscriber.open(srconfig);
			
				feeds = results.get(subscriberId);
				subscriber.subscribe(feeds);
				
			}
			
			//Start the Streams
			for (String streamId : streams.keySet()) {
				logger.info("Stream Manager - Start Stream : " + streamId);
				StreamConfiguration sconfig = config.getStreamConfig(streamId);
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
				
				if(monitor != null) {
					monitor.addStream(streamId, stream, feeds);
					monitor.startStream(streamId);
				}
			}
			
			if(monitor != null && monitor.getNumberOfStreamFetchTasks() > 0) {
				monitor.startReInitializer();
			}

		}
		catch(Exception e) {
			e.printStackTrace();
			throw new StreamException("Error during streams open", e);
		}
	}
	
	/**
	 * Closes Manager and its auxiliary modules
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
		try{
			for (String streamId : config.getStreamIds()){
				StreamConfiguration sconfig = config.getStreamConfig(streamId);
				streams.put(streamId,(Stream)Class.forName(sconfig.getParameter(StreamConfiguration.CLASS_PATH)).newInstance());
			}
		}catch(Exception e) {
			e.printStackTrace();
			throw new StreamException("Error during streams initialization", e);
		}
	}
	
	/**
	 * Initializes the streams apis, that implement subscriber channels, that are going to be searched for 
	 * relevant content
	 * @throws StreamException
	 */
	private void initSubscribers() throws StreamException {
		
		subscribers = new HashMap<String, Subscriber>();
		try {
			for (String subscriberId : config.getSubscriberIds()) {
				StreamConfiguration sconfig = config.getSubscriberConfig(subscriberId);
				Subscriber subscriber = (Subscriber) Class.forName(sconfig.getParameter(StreamConfiguration.CLASS_PATH)).newInstance();
				subscribers.put(subscriberId, subscriber);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new StreamException("Error during Subscribers initialization", e);
		}
	}
}
