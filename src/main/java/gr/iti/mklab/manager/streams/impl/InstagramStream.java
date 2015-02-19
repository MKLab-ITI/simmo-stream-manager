package gr.iti.mklab.manager.streams.impl;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.retrievers.impl.InstagramRetriever;
import gr.iti.mklab.manager.config.Configuration;
import gr.iti.mklab.manager.streams.Stream;

/**
 * Class responsible for setting up the connection to Instagram API
 * for retrieving relevant Instagram content.
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 */

public class InstagramStream extends Stream {
	
	private Logger logger = Logger.getLogger(InstagramStream.class);
	
	public static final String SOURCE = "Instagram";


	@Override
	public void open(Configuration config) throws Exception {
		logger.info("#Instagram : Open stream");
		
		if (config == null) {
			logger.error("#Instagram : Config file is null.");
			return;
		}
		
		String key = config.getParameter(KEY);
		String secret = config.getParameter(SECRET);
		String token = config.getParameter(ACCESS_TOKEN);
		
		String maxResults = config.getParameter(MAX_RESULTS);
		String maxRequests = config.getParameter(MAX_REQUESTS);
		String maxRunningTime = config.getParameter(MAX_RUNNING_TIME);
		
		if (key == null || secret == null || token == null) {
			logger.error("#Instagram : Stream requires authentication.");
			throw new Exception("Stream requires authentication.");
		}
		
		Credentials credentials = new Credentials();
		credentials.setKey(key);
		credentials.setSecret(secret);
		credentials.setAccessToken(token);
		
		//RateLimitsMonitor rateLimitsMonitor = new RateLimitsMonitor(Integer.parseInt(maxRequests), Long.parseLong(maxRunningTime));
		
		retriever = new InstagramRetriever(credentials);
	
	}


	@Override
	public String getName() {
		return "Instagram";
	}
	
}

