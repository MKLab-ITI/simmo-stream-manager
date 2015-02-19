package gr.iti.mklab.manager.streams.impl;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.retrievers.impl.FlickrRetriever;
import gr.iti.mklab.manager.config.Configuration;
import gr.iti.mklab.manager.streams.Stream;

/**
 * Class responsible for setting up the connection to Flickr API
 * for retrieving relevant Flickr content.
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 */
public class FlickrStream extends Stream {

	private Logger logger = Logger.getLogger(FlickrStream.class);
	
	public static final String SOURCE = "Flickr";
	
	private String key;
	private String secret;

	
	@Override
	public void open(Configuration config) throws Exception {
		logger.info("#Flickr : Open stream");
		
		if (config == null) {
			logger.error("#Flickr : Config file is null.");
			return;
		}
		
		key = config.getParameter(KEY);
		secret = config.getParameter(SECRET);
		
		String maxResults = config.getParameter(MAX_RESULTS);
		String maxRequests = config.getParameter(MAX_REQUESTS);
		
		if (key == null || secret==null) {
			logger.error("#Flickr : Stream requires authentication.");
			throw new Exception("Stream requires authentication.");
		}
		
		Credentials credentials = new Credentials();
		credentials.setKey(key);
		credentials.setSecret(secret);
		
		//RateLimitsMonitor rateLimitsMonitor = new RateLimitsMonitor(Integer.parseInt(maxResults), Long.parseLong(maxRequests));
		
		retriever = new FlickrRetriever(credentials);
		
	}
	
	@Override
	public String getName() {
		return "Flickr";
	}
	
}
