package gr.iti.mklab.manager.streams.impl;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.retrievers.impl.TumblrRetriever;
import gr.iti.mklab.manager.config.Configuration;
import gr.iti.mklab.manager.streams.Stream;

/**
 * Class responsible for setting up the connection to Tumblr API
 * for retrieving relevant Tumblr content.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class TumblrStream extends Stream {
	
	public static final String SOURCE = "Tumblr";
	
	private String consumerKey;
	private String consumerSecret;
	
	private Logger logger = Logger.getLogger(TumblrStream.class);

	
	@Override
	public void open(Configuration config) throws Exception {
		logger.info("#Tumblr : Open stream");
		
		if (config == null) {
			logger.error("#Tumblr : Config file is null.");
			return;
		}
		
		consumerKey = config.getParameter(KEY);
		consumerSecret = config.getParameter(SECRET);
		
		String maxResults = config.getParameter(MAX_RESULTS);
		String maxRequests = config.getParameter(MAX_REQUESTS);
		String maxRunningTime = config.getParameter(MAX_RUNNING_TIME);
		
		if (consumerKey == null || consumerSecret==null) {
			logger.error("#Tumblr : Stream requires authentication.");
			throw new Exception("Stream requires authentication.");
		}
		
		Credentials credentials = new Credentials();
		credentials.setKey(consumerKey);
		credentials.setSecret(consumerSecret);
		
		//RateLimitsMonitor rateLimitsMonitor = new RateLimitsMonitor(Integer.parseInt(maxRequests), Long.parseLong(maxRunningTime));
		
		retriever = new TumblrRetriever(credentials);
		
	}

	@Override
	public String getName() {
		return SOURCE;
	}
	
}
