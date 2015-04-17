package gr.iti.mklab.sm.streams.impl;

import gr.iti.mklab.sm.Configuration;
import gr.iti.mklab.sm.retrievers.impl.RssRetriever;
import gr.iti.mklab.sm.streams.Stream;

/**
 * Class responsible for setting up the connection for retrieving RSS feeds.
 * 
 * @author Manos Schinas
 * 
 * @email  manosetro@iti.gr
 */
public class RssStream extends Stream {
	
	public static String SOURCE = "RSS";
	
	@Override
	public void open(Configuration config) {
		
		this.maxRequests = Integer.MAX_VALUE;
		this.timeWindow = Integer.parseInt(config.getParameter(TIME_WINDOW));
		
		retriever = new RssRetriever();
	}

	@Override
	public String getName() {
		return SOURCE;
	}
	
}
