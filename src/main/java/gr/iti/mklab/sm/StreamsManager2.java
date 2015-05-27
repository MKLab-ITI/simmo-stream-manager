package gr.iti.mklab.sm;

import gr.iti.mklab.sm.feeds.Feed;
import gr.iti.mklab.sm.feeds.GeoFeed;
import gr.iti.mklab.sm.feeds.KeywordsFeed;
import gr.iti.mklab.sm.management.StorageHandler;
import gr.iti.mklab.sm.streams.Stream;
import gr.iti.mklab.sm.streams.StreamException;
import gr.iti.mklab.sm.streams.StreamsManagerConfiguration;
import gr.iti.mklab.sm.streams.monitors.StreamsMonitor;
import gr.iti.mklab.sm.subscribers.Subscriber;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * This is a variation of the StreamsManager class, which does not need to read
 * the feeds specification from a database. The feeds are added programmatically,
 * see addKeywordsFeed and addGeoFeed methods
 *
 * @author kandreadou
 */
public class StreamsManager2 implements Runnable {

    public final Logger logger = Logger.getLogger(StreamsManager2.class);

    enum ManagerState {
        OPEN, CLOSE
    }

    private Map<String, Stream> streams = null;
    private Map<String, Subscriber> subscribers = null;

    private StreamsManagerConfiguration config = null;
    private StorageHandler storageHandler;

    private StreamsMonitor monitor;

    private ManagerState state = ManagerState.CLOSE;

    private Set<Feed> feeds = new HashSet<Feed>();

    public StreamsManager2(StreamsManagerConfiguration config) throws StreamException {

        if (config == null) {
            throw new StreamException("Manager's configuration must be specified");
        }

        //Set the configuration files
        this.config = config;

        //Set up the Subscribers
        initSubscribers();

        //Set up the Streams
        initStreams();
    }

    /**
     * Opens Manager by starting the auxiliary modules and setting up
     * the database for reading/storing
     *
     * @throws StreamException
     */
    public synchronized void open(Set<String> keywords) throws StreamException {

        if (state == ManagerState.OPEN) {
            return;
        }

        state = ManagerState.OPEN;
        logger.info("StreamsManager is open now.");

        try {
            //If there are Streams to monitor start the StreamsMonitor
            if (streams != null && !streams.isEmpty()) {
                monitor = new StreamsMonitor(streams.size());
            } else {
                throw new StreamException("There are no streams to open.");
            }

            //Start stream handler
            storageHandler = new StorageHandler(config);
            storageHandler.start();
            logger.info("Storage Manager is ready to store.");

            //addKeywordFeeds(keywords);
            addGeoFeed();

            Map<String, Set<Feed>> feedsPerSource = createFeedsPerSource(feeds);

            //Start the Subscribers
            for (String subscriberId : subscribers.keySet()) {
                logger.info("Stream Manager - Start Subscriber : " + subscriberId);
                Configuration srconfig = config.getSubscriberConfig(subscriberId);
                Subscriber subscriber = subscribers.get(subscriberId);

                subscriber.setHandler(storageHandler);
                subscriber.open(srconfig);

                Set<Feed> sourceFeed = feedsPerSource.get(subscriberId);
                subscriber.subscribe(sourceFeed);
            }

            //Start the Streams
            for (String streamId : streams.keySet()) {
                logger.info("Start Stream : " + streamId);
                Configuration sconfig = config.getStreamConfig(streamId);
                Stream stream = streams.get(streamId);
                stream.setHandler(storageHandler);
                stream.open(sconfig);

                monitor.addStream(stream);
            }
            monitor.start();

        } catch (Exception e) {
            e.printStackTrace();
            throw new StreamException("Error during streams open", e);
        }
    }

    /**
     * Closes Manager and its auxiliary modules
     *
     * @throws StreamException
     */
    public synchronized void close() throws StreamException {

        if (state == ManagerState.CLOSE) {
            logger.info("StreamManager is already closed.");
            return;
        }

        try {
            if (monitor != null) {
                monitor.stop();
            }

            if (storageHandler != null) {
                storageHandler.stop();
            }

            state = ManagerState.CLOSE;
        } catch (Exception e) {
            throw new StreamException("Error during streams close", e);
        }
    }

    /**
     * Initializes the streams apis that are going to be searched for
     * relevant content
     *
     * @throws StreamException
     */
    private void initStreams() throws StreamException {
        streams = new HashMap<String, Stream>();
        try {
            for (String streamId : config.getStreamIds()) {
                Configuration sconfig = config.getStreamConfig(streamId);
                Stream stream = (Stream) Class.forName(sconfig.getParameter(Configuration.CLASS_PATH)).newInstance();
                streams.put(streamId, stream);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new StreamException("Error during streams initialization", e);
        }
    }

    /**
     * Initializes the streams apis, that implement subscriber channels, that are going to be searched for
     * relevant content
     *
     * @throws StreamException
     */
    private void initSubscribers() throws StreamException {
        subscribers = new HashMap<String, Subscriber>();
        try {
            for (String subscriberId : config.getSubscriberIds()) {
                Configuration sconfig = config.getSubscriberConfig(subscriberId);
                Subscriber subscriber = (Subscriber) Class.forName(sconfig.getParameter(Configuration.CLASS_PATH)).newInstance();
                subscribers.put(subscriberId, subscriber);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new StreamException("Error during Subscribers initialization", e);
        }
    }

    @Override
    public void run() {

        if (state != ManagerState.OPEN) {
            logger.error("Streams Manager is not open!");
            return;
        }

        for (Feed feed : feeds) {
            String streamId = feed.getSource();
            if (monitor != null) {
                Stream stream = monitor.getStream(streamId);
                if (stream != null) {
                    monitor.addFeed(streamId, feed);
                } else {
                    logger.error("Stream " + streamId + " has not initialized");
                }
            }
        }
    }

    public Map<String, Set<Feed>> createFeedsPerSource(Set<Feed> allFeeds) {

        Map<String, Set<Feed>> feedsPerSource = new HashMap<String, Set<Feed>>();

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

    private void addGeoFeed() {

        double lat1 = 48.837379;
        double lon1 = 2.282352;
        double lat2 = 48.891358;
        double lon2 = 2.394619;
        double density = 0.02;
        GeoFeed feed = new GeoFeed(lon1, lat1, lon2, lat2, density);
        feed.setId("Panoramio#1");
        feed.setSource("Panoramio");
        feeds.add(feed);
    }

    private void addKeywordFeeds(Set<String> keywords) {
        KeywordsFeed feed = new KeywordsFeed();
        feed.addKeywords(new ArrayList(keywords));
        feed.setId("Twitter#1");
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, -1);
        feed.setSinceDate(cal.getTime());
        feed.setSource("Twitter");
        feeds.add(feed);
        KeywordsFeed youtube = new KeywordsFeed();
        youtube.addKeywords(new ArrayList(keywords));
        youtube.setId("Youtube#1");
        youtube.setSinceDate(cal.getTime());
        youtube.setSource("YouTube");
        feeds.add(youtube);
        KeywordsFeed flickr = new KeywordsFeed();
        flickr.addKeywords(new ArrayList(keywords));
        flickr.setId("Flickr#1");
        flickr.setSinceDate(cal.getTime());
        flickr.setSource("Flickr");
        feeds.add(flickr);
        KeywordsFeed instagram = new KeywordsFeed();
        instagram.addKeywords(new ArrayList(keywords));
        instagram.setId("Instagram#1");
        instagram.setSinceDate(cal.getTime());
        instagram.setSource("Instagram");
        feeds.add(instagram);
        KeywordsFeed tumblr = new KeywordsFeed();
        tumblr.addKeywords(new ArrayList(keywords));
        tumblr.setId("Tumblr#1");
        tumblr.setSinceDate(cal.getTime());
        tumblr.setSource("Tumblr");
        feeds.add(tumblr);
    }

    public static void main(String[] args) throws Exception {

        File streamConfigFile;
        if (args.length != 1) {
            streamConfigFile = new File("/home/kandreadou/mklab/streams.geo.xml");
        } else {
            streamConfigFile = new File(args[0]);
        }

        StreamsManager2 manager = null;
        try {
            StreamsManagerConfiguration config = StreamsManagerConfiguration.readFromFile(streamConfigFile);
            //config.getStorageConfig("Mongodb").setParameter("mongodb.database", dbname);

            Set<String> set = new HashSet<String>();
            set.add("snowden");
            set.add("assange");
            manager = new StreamsManager2(config);
            manager.open(set);

            //Runtime.getRuntime().addShutdownHook(new Shutdown(manager));

            Thread thread = new Thread(manager);
            thread.start();


        } catch (ParserConfigurationException e) {
            System.out.println(e);
        } catch (SAXException e) {
            System.out.println(e);
        } catch (IOException e) {
            System.out.println(e);
        } catch (StreamException e) {
            System.out.println(e);
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
