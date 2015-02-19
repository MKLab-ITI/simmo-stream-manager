package gr.iti.mklab.manager.input;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.QueryResults;

import com.mongodb.MongoClient;

import gr.iti.mklab.framework.feeds.AccountFeed;
import gr.iti.mklab.framework.feeds.Feed;
import gr.iti.mklab.framework.feeds.KeywordsFeed;
import gr.iti.mklab.manager.config.Configuration;

/**
 * @brief The class responsible for the creation of input feeds from mongo db storage
 * @author manosetro
 * @email  manosetro@iti.gr
 */
public class FeedsCreator {
	
	private BasicDAO<Feed, String> feedsDAO;
	
	public FeedsCreator(Configuration config) throws UnknownHostException {	
		String hostname = config.getParameter("hostname");
		String dbName = config.getParameter("database");
		 
		Morphia morphia = new Morphia();
		MongoClient client = new MongoClient(hostname);
	       
		feedsDAO = new BasicDAO<Feed, String>(Feed.class, client, morphia, dbName);
	}

	public Map<String, List<Feed>> createFeedsPerSource() {
		HashMap<String, List<Feed>> feedsPerSource = new HashMap<String, List<Feed>>();
		List<Feed> feeds = createFeeds();
		for(Feed feed : feeds) {	
			String source = feed.getSource();
			List<Feed> sourceFeeds = feedsPerSource.get(source);
			if(sourceFeeds == null) {
				sourceFeeds = new ArrayList<Feed>();
				feedsPerSource.put(source, sourceFeeds);
			}
			sourceFeeds.add(feed);
		}
		return feedsPerSource;
	}
	
	public void save(Feed feed) {
		feedsDAO.save(feed);
	}
	
	public List<Feed> createFeeds() {

		Query<Feed> query = feedsDAO.createQuery();
		
		QueryResults<Feed> feedsResults = feedsDAO.find(query);
		List<Feed> feeds = feedsResults.asList();
		
		return feeds;
	}

	public static void main(String...args) throws UnknownHostException {
		
		Configuration conf = new Configuration();
		conf.setParameter("hostname", "160.40.50.207");
		conf.setParameter("database", "SM2_test");
		
		FeedsCreator feedsCreator = new FeedsCreator(conf);
		
		Date since = new Date(System.currentTimeMillis()-9600000);
		Feed feed1 = new AccountFeed("398789134", "ShabuQureshi", since);
		Feed feed2 = new KeywordsFeed("1", "obama", since);
		
		feedsCreator.save(feed1);
		feedsCreator.save(feed2);
		
		List<Feed> feeds = feedsCreator.createFeeds();
		System.out.println(feeds.size() + " feeds found!");
		for(Feed feed : feeds) {
			System.out.println(feed.getClass());
		}
	}
}