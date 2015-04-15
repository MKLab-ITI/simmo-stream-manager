package gr.iti.mklab.simmo.sfc.subscribers;

import java.util.Set;

import gr.iti.mklab.framework.feeds.Feed;
import gr.iti.mklab.simmo.sfc.streams.Stream;
import gr.iti.mklab.simmo.sfc.streams.StreamException;

/**
 * The interface for retrieving content by subscribing to a social network channel.
 * Currently the only API that supports subscribing is Twitter API.
 * @author ailiakop
 *
 */
public abstract class Subscriber extends Stream {

	/**
	 * Retrieves and stores relevant real-time content to a list of feeds by subscribing
	 * to a social network channel. 
	 * @param feed
	 * @throws StreamException
	 */
	public abstract void subscribe(Set<Feed> feeds) throws StreamException;

	public abstract void stop();

}
