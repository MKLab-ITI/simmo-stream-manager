package gr.iti.mklab.sm.streams.impl;

import gr.iti.mklab.simmo.impl.Sources;
import gr.iti.mklab.sm.Configuration;
import gr.iti.mklab.sm.Credentials;
import gr.iti.mklab.sm.retrievers.impl.YoutubeRetriever;

import org.apache.log4j.Logger;

import gr.iti.mklab.sm.streams.Stream;
import gr.iti.mklab.sm.streams.StreamException;
import gr.iti.mklab.sm.streams.monitors.RateLimitsMonitor;

/**
 * Class responsible for setting up the connection to Google API
 * for retrieving relevant YouTube content.
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 */
public class YoutubeStream extends Stream {

	public static String SOURCE = Sources.YOUTUBE;
	
	private Logger logger = Logger.getLogger(YoutubeStream.class);
	
	private String clientId;
	private String developerKey;
	
	@Override
	public void close() throws StreamException {
		logger.info("#YouTube : Close stream");
	}
	
	@Override
	public void open(Configuration config) throws StreamException {
		logger.info("#YouTube : Open stream");
		
		if (config == null) {
			logger.error("#YouTube : Config file is null.");
			return;
		}
		
		this.clientId = config.getParameter(CLIENT_ID);
		this.developerKey = config.getParameter(KEY);
		
		if (clientId == null || developerKey == null) {
			logger.error("#YouTube : Stream requires authentication.");
			throw new StreamException("Stream requires authentication");
		}

		Credentials credentials = new Credentials();
		credentials.setKey(developerKey);
		credentials.setClientId(clientId);
		
		maxRequests = Integer.parseInt(config.getParameter(MAX_REQUESTS));
		timeWindow = Long.parseLong(config.getParameter(TIME_WINDOW));
		
		rateLimitsMonitor = new RateLimitsMonitor(maxRequests, timeWindow);
		try {
			retriever = new YoutubeRetriever(credentials);
		} catch (Exception e) {
			throw new StreamException(e);
		}
	}
	
	@Override
	public String getName() {
		return SOURCE;
	}
	
}
