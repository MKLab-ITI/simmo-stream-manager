package gr.iti.mklab.manager.streams.impl;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.retrievers.impl.FacebookRetriever;
import gr.iti.mklab.manager.config.Configuration;
import gr.iti.mklab.manager.streams.Stream;

/**
 * Class responsible for setting up the connection to Facebook API
 * for retrieving relevant Facebook content.
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 */
public class FacebookStream extends Stream {
	
	public static String SOURCE = "Facebook";
	
	public int maxFBRequests = 600;
	public long minInterval = 600000;
	
	private Logger  logger = Logger.getLogger(FacebookStream.class);	
	
	@Override
	public synchronized void open(Configuration config) throws Exception {
		logger.info("#Facebook : Open stream");
		
		if (config == null) {
			logger.error("#Facebook : Config file is null.");
			return;
		}
		
		
		String accessToken = config.getParameter(ACCESS_TOKEN);
		String app_id = config.getParameter(APP_ID);
		String app_secret = config.getParameter(APP_SECRET);
		
		int maxRequests = Integer.parseInt(config.getParameter(MAX_REQUESTS));
		
		if(maxRequests > maxFBRequests)
			maxRequests = maxFBRequests;   
		
		if (accessToken == null && app_id == null && app_secret == null) {
			logger.error("#Facebook : Stream requires authentication.");
			throw new Exception("Stream requires authentication.");
		}
		
		
		if(accessToken == null)
			accessToken = app_id+"|"+app_secret;
		
		Credentials credentials = new Credentials();
		credentials.setAccessToken(accessToken);
		
		//RateLimitsMonitor rateLimitsMonitor = new RateLimitsMonitor(maxRequests, minInterval);
		
		retriever = new FacebookRetriever(credentials);	

	}

	@Override
	public String getName() {
		return "Facebook";
	}
	
}
