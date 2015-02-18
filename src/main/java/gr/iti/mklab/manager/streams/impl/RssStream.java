package gr.iti.mklab.sfc.streams.impl;

import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.retrievers.impl.RssRetriever;
import gr.iti.mklab.sfc.streams.Stream;
import gr.iti.mklab.sfc.streams.StreamConfiguration;

/**
 * Class responsible for setting up the connection for retrieving RSS feeds.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class RssStream extends Stream {
	
	public static Source SOURCE = Source.RSS;
	
	@Override
	public void open(StreamConfiguration config) {
		retriever = new RssRetriever();
	}

	@Override
	public String getName() {
		return "RSS";
	}

}
