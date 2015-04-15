package gr.iti.mklab.simmo.sfc.streams.impl;


import org.apache.log4j.Logger;


import gr.iti.mklab.framework.Configuration;
import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.abstractions.socialmedia.Sources;
import gr.iti.mklab.framework.retrievers.impl.FacebookRetriever;
import gr.iti.mklab.simmo.sfc.streams.Stream;
import gr.iti.mklab.simmo.sfc.streams.StreamException;
import gr.iti.mklab.simmo.sfc.streams.monitors.RateLimitsMonitor;

/**
 * Class responsible for setting up the connection to Facebook API
 * for retrieving relevant Facebook content.
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 */
public class FacebookStream extends Stream {
	
	public static String SOURCE = Sources.FACEBOOK;

	private Logger  logger = Logger.getLogger(FacebookStream.class);	
	
	@Override
	public synchronized void open(Configuration config) throws StreamException {
		logger.info("#Facebook : Open stream");
		
		if (config == null) {
			logger.error("#Facebook : Config file is null.");
			return;
		}
		
		
		String accessToken = config.getParameter(ACCESS_TOKEN);
		String key = config.getParameter(KEY);
		String secret = config.getParameter(SECRET);
		
		if (accessToken == null && key == null && secret == null) {
			logger.error("#Facebook : Stream requires authentication.");
			throw new StreamException("Stream requires authentication.");
		}
			
		if(accessToken == null) {
			accessToken = key + "|" + secret;
		}
		
		Credentials credentials = new Credentials();
		credentials.setAccessToken(accessToken);
		
		maxRequests = Integer.parseInt(config.getParameter(MAX_REQUESTS));
		timeWindow = Long.parseLong(config.getParameter(TIME_WINDOW));
		
		rateLimitsMonitor = new RateLimitsMonitor(maxRequests, timeWindow);
		try {
			retriever = new FacebookRetriever(credentials);
		} catch (Exception e) {
			throw new StreamException(e);
		}	
	}

	@Override
	public String getName() {
		return SOURCE;
	}
	
	
}
