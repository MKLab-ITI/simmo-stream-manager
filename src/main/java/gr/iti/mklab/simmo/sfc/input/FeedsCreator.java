package gr.iti.mklab.simmo.sfc.input;

import gr.iti.mklab.framework.Configuration;
import gr.iti.mklab.framework.feeds.Feed;
import gr.iti.mklab.simmo.morphia.MorphiaManager;
import gr.iti.mklab.simmo.morphia.ObjectDAO;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mongodb.morphia.query.QueryResults;


/**
 * @brief The class responsible for the creation of input feeds from mongodb storage
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 */
public class FeedsCreator {
	
	protected static final String SINCE = "since";
	
	protected static final String HOST = "host";
	protected static final String DB = "database";
	
	private String host = null;
	private String db = null;
	
	private ObjectDAO<Feed> feedsDao;
	
	public FeedsCreator(Configuration config) throws Exception {
		this.host = config.getParameter(HOST);
		this.db = config.getParameter(DB);
		
		MorphiaManager.setup(host);
		feedsDao = new ObjectDAO<Feed>(Feed.class, db);
	}
	
	public Map<String, Set<Feed>> createFeedsPerSource() {
	
		Map<String, Set<Feed>> feedsPerSource = new HashMap<String, Set<Feed>>();
		
		Set<Feed> allFeeds = createFeeds();
		for(Feed feed : allFeeds) {
			String source = feed.getSource();
			Set<Feed> feeds = feedsPerSource.get(source);
			if(feeds == null) {
				feeds = new HashSet<Feed>();
				feedsPerSource.put(source, feeds);
			}	
			feeds.add(feed);
		}
		
		return feedsPerSource;
	}

	public Set<Feed> createFeeds() {
		QueryResults<Feed> result = feedsDao.find();
		List<Feed> feeds = result.asList();
		
		return new HashSet<Feed>(feeds);
	}
	
	public static void main(String...args) {
		
	}
	
}