package gr.iti.mklab.sm.streams.impl;

import gr.iti.mklab.simmo.impl.Sources;
import gr.iti.mklab.sm.Configuration;
import gr.iti.mklab.sm.Credentials;
import gr.iti.mklab.sm.retrievers.impl.InstagramRetriever;

import org.apache.log4j.Logger;

import gr.iti.mklab.sm.streams.Stream;
import gr.iti.mklab.sm.streams.StreamException;
import gr.iti.mklab.sm.streams.monitors.RateLimitsMonitor;

/**
 * Class responsible for setting up the connection to Instagram API
 * for retrieving relevant Instagram content.
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 */

public class InstagramStream extends Stream {
	
	private Logger logger = Logger.getLogger(InstagramStream.class);
	
	public static final String SOURCE = Sources.INSTAGRAM;


	@Override
	public void open(Configuration config) throws StreamException {
		logger.info("#Instagram : Open stream");
		
		if (config == null) {
			logger.error("#Instagram : Config file is null.");
			return;
		}
		
		String key = config.getParameter(KEY);
		String secret = config.getParameter(SECRET);
		String token = config.getParameter(ACCESS_TOKEN);
		
		if (key == null || secret == null || token == null) {
			logger.error("#Instagram : Stream requires authentication.");
			throw new StreamException("Stream requires authentication.");
		}
		
		Credentials credentials = new Credentials();
		credentials.setKey(key);
		credentials.setSecret(secret);
		credentials.setAccessToken(token);
		
		maxRequests = Integer.parseInt(config.getParameter(MAX_REQUESTS));
		timeWindow = Long.parseLong(config.getParameter(TIME_WINDOW));
		
		rateLimitsMonitor = new RateLimitsMonitor(maxRequests, timeWindow);
		try {
			retriever = new InstagramRetriever(credentials);
		} catch (Exception e) {
			throw new StreamException(e);
		}
		
	}

	@Override
	public String getName() {
		return SOURCE;
	}
}

