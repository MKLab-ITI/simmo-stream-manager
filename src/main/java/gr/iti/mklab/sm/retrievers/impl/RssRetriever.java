package gr.iti.mklab.sm.retrievers.impl;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import gr.iti.mklab.simmo.core.documents.Post;
import gr.iti.mklab.simmo.impl.posts.RSSPost;
import gr.iti.mklab.sm.feeds.Feed;
import gr.iti.mklab.sm.feeds.RssFeed;
import gr.iti.mklab.sm.retrievers.Retriever;

import org.apache.log4j.Logger;

import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;

import gr.iti.mklab.sm.retrievers.Response;

/**
 * Class for retrieving rss feeds from official sources
 * The retrieval process takes place through ROME API. 
 * @author ailiakop
 */
public class RssRetriever implements Retriever {
	
	public final Logger logger = Logger.getLogger(RssRetriever.class);
	
	private long oneMonthPeriod = 2592000000L;
	
	@Override
	public Response retrieve(Feed feed) throws Exception {
		return retrieve(feed, 1);
	}
		
	@Override
	public Response retrieve(Feed feed, Integer maxRequests) throws Exception {
		
		Response response = new Response();
		List<Post> items = new ArrayList<Post>();
		
		RssFeed ufeed = (RssFeed) feed;
		System.out.println("["+new Date()+"] Retrieving RSS Feed: " + ufeed.getURL());
		
		Integer totalRetrievedItems = 0;
		if(ufeed.getURL().equals(""))
			return response;
			
		URL url = null;
		try {
			url = new URL(ufeed.getURL());
		} catch (MalformedURLException e) {
			logger.error(e);
			return response;
		}
			
		XmlReader reader;
		try {
			reader = new XmlReader(url);
			SyndFeed rssData = new SyndFeedInput().build(reader);
			
			@SuppressWarnings("unchecked")
			List<SyndEntry> rssEntries = rssData.getEntries();
			
		
			for (SyndEntry rss : rssEntries) {		
				if(rss.getLink() != null) {
							
					if(rss.getPublishedDate() != null && rss.getPublishedDate().getTime()>0 && 
							Math.abs(System.currentTimeMillis() - rss.getPublishedDate().getTime())<oneMonthPeriod) {
								
						Post rssItem = new RSSPost(rss);
								
						//String label = feed.getLabel();
						//rssItem.setList(label);
						
						items.add(rssItem);	
						totalRetrievedItems++;
						
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							logger.error(e);
							continue;
						}
					}
				}
			}
		} catch (IOException e) {
			logger.error(e);
		} catch (Exception e) {
			logger.error(e);
		}
	
		response.setPosts(items);
		return response;
	}
	
	public static void main(String...args) throws Exception {
		RssRetriever retriever = new RssRetriever();
		
		Feed feed = new RssFeed("ecowatch", "http://ecowatch.com/feed/", new Date(System.currentTimeMillis()-3600000));
		
		retriever.retrieve(feed);
	}
	
}
