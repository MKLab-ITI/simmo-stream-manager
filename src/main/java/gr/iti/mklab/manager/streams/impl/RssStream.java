package gr.iti.mklab.manager.streams.impl;

import gr.iti.mklab.framework.retrievers.impl.RssRetriever;
import gr.iti.mklab.manager.config.Configuration;
import gr.iti.mklab.manager.streams.Stream;

/**
 * Class responsible for setting up the connection for retrieving RSS feeds.
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 */
public class RssStream extends Stream {
	
	public static String SOURCE = "RSS";
	
	@Override
	public void open(Configuration config) {
		retriever = new RssRetriever();
	}

	@Override
	public String getName() {
		return SOURCE;
	}

}
