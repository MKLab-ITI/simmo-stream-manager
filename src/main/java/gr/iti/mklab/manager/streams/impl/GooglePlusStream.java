package gr.iti.mklab.manager.streams.impl;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.retrievers.impl.GooglePlusRetriever;
import gr.iti.mklab.manager.config.Configuration;
import gr.iti.mklab.manager.streams.Stream;

/**
 * Class responsible for setting up the connection to Google API
 * for retrieving relevant Google+ content.
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 */
public class GooglePlusStream extends Stream {
	
	public static final String SOURCE = "GooglePlus";
	
	private Logger logger = Logger.getLogger(GooglePlusStream.class);
	
	private String key;

	@Override
	public void open(Configuration config) throws Exception {
		logger.info("#GooglePlus : Open stream");
		
		if (config == null) {
			logger.error("#GooglePlus : Config file is null.");
			return;
		}
		
		key = config.getParameter(KEY);
		
		String maxResults = config.getParameter(MAX_RESULTS);
		String maxRunningTime = config.getParameter(MAX_RUNNING_TIME);
		
		if (key == null) {
			logger.error("#GooglePlus : Stream requires authentication.");
			throw new Exception("Stream requires authentication.");
		}
		
		Credentials credentials = new Credentials();
		credentials.setKey(key);
		
		//RateLimitsMonitor rateLimitsMonitor = new RateLimitsMonitor(Integer.parseInt(maxResults), Long.parseLong(maxRunningTime));
		
		retriever = new GooglePlusRetriever(credentials);
		
	}
	
	@Override
	public String getName() {
		return "GooglePlus";
	}
}
