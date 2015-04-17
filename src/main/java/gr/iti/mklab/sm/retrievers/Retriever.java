package gr.iti.mklab.sm.retrievers;

import gr.iti.mklab.sm.feeds.Feed;

public interface Retriever {
	
	/**
	 * Retrieves a feed that is inserted into the system (Feeds currently supported
	 * by the platform are: KeywordFeeds,LocationFeeds,SourceFeeds,ListFeeds,URLFeeds)
	 * 
	 * @param feed - An input feed
	 * @return gr.iti.mklab.framework.retrievers.Response
	 * 
	 * @throws Exception during any retrieval errors
	 */
	public Response retrieve(Feed feed) throws Exception;
	
	public Response retrieve(Feed feed, Integer maxRequests) throws Exception;
	
}
