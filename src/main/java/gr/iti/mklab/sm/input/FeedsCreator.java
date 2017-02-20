package gr.iti.mklab.sm.input;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import gr.iti.mklab.sm.Configuration;
import gr.iti.mklab.sm.feeds.Feed;

import org.mongodb.morphia.Key;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.query.QueryResults;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;


/**
 * @author manosetro
 * @brief The class responsible for the creation of input feeds from mongodb storage
 * @email manosetro@iti.gr
 */
public class FeedsCreator {

    protected static final String SINCE = "since";

    protected static final String HOST = "host";
    protected static final String DB = "database";
    protected static final String USERNAME = "username";
    protected static final String PASSWORD = "password";
    
    Morphia morphia = new Morphia();

    private String host = null;
    private String db = null;

    private String username = null;
    private String password = null;
    
    private BasicDAO<Feed, String> feedsDao;

    public FeedsCreator(Configuration config) throws Exception {
        morphia.map(Feed.class);

        this.host = config.getParameter(HOST);
        this.db = config.getParameter(DB);
        
        this.username = config.getParameter(USERNAME);
        this.password = config.getParameter(PASSWORD);

        MongoClient mongoClient;
        if(username != null && !username.equals("") && password != null && !password.equals("")) {
            MongoClientOptions options = MongoClientOptions.builder().build();
        	ServerAddress srvAdr = new ServerAddress(host != null ? host : "localhost" , 27017);
        	MongoCredential credential = MongoCredential.createScramSha1Credential(username, "admin", password.toCharArray());
        	
        	mongoClient = new MongoClient(srvAdr, Arrays.asList(credential), options);
        }
        else {
        	mongoClient = new MongoClient(host);
        }

        

        feedsDao = new BasicDAO<>(Feed.class, mongoClient, morphia, db);
        // ensure capped collections
        feedsDao.getDatastore().ensureCaps();
    }

    public Map<String, Set<Feed>> createFeedsPerSource() {

        Map<String, Set<Feed>> feedsPerSource = new HashMap<String, Set<Feed>>();

        Set<Feed> allFeeds = createFeeds();
        for (Feed feed : allFeeds) {
            String source = feed.getSource();
            Set<Feed> feeds = feedsPerSource.get(source);
            if (feeds == null) {
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

    public String addFeed(Feed feed) {
        return feedsDao.save(feed).toString();
    }

    public String deleteFeed(String id) {
        return feedsDao.deleteById(id).toString();
    }

    public Object saveFeed(Feed feed) {
    	Key<Feed> result = feedsDao.save(feed);
    	return result.getId();
    }
}