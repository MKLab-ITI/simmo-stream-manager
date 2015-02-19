package gr.iti.mklab.manager.streams.impl;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.retrievers.impl.YoutubeRetriever;
import gr.iti.mklab.manager.config.Configuration;
import gr.iti.mklab.manager.streams.Stream;

/**
 * Class responsible for setting up the connection to Google API
 * for retrieving relevant YouTube content.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class YoutubeStream extends Stream {

	public static String SOURCE = "Youtube";
	
	private Logger logger = Logger.getLogger(YoutubeStream.class);
	
	private String clientId;
	private String developerKey;
	
	@Override
	public void open(Configuration config) throws Exception {
		logger.info("#YouTube : Open stream");
		
		if (config == null) {
			logger.error("#YouTube : Config file is null.");
			return;
		}
		
		this.clientId = config.getParameter(CLIENT_ID);
		this.developerKey = config.getParameter(KEY);
		String maxResults = config.getParameter(MAX_RESULTS);
		String maxRunningTime = config.getParameter(MAX_RUNNING_TIME);
		
		if (clientId == null || developerKey == null) {
			logger.error("#YouTube : Stream requires authentication.");
			throw new Exception("Stream requires authentication");
		}

		Credentials credentials = new Credentials();
		credentials.setKey(developerKey);
		credentials.setClientId(clientId);
		
		//RateLimitsMonitor rateLimitsMonitor = new RateLimitsMonitor(Integer.parseInt(maxResults), Long.parseLong(maxRunningTime));
		
		retriever = new YoutubeRetriever(credentials);

	}
	
	@Override
	public String getName() {
		return "YouTube";
	}
	
}
