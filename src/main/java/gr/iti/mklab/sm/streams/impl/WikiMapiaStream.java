package gr.iti.mklab.sm.streams.impl;

import gr.iti.mklab.simmo.impl.Sources;
import gr.iti.mklab.sm.Configuration;
import gr.iti.mklab.sm.Credentials;
import gr.iti.mklab.sm.retrievers.impl.WikiMapiaRetriever;
import gr.iti.mklab.sm.streams.Stream;
import gr.iti.mklab.sm.streams.StreamException;
import gr.iti.mklab.sm.streams.monitors.RateLimitsMonitor;
import org.apache.log4j.Logger;

/**
 * Class responsible for setting up the connection to WikiMapia
 * for retrieving relevant WikiMapia content.
 *
 * @author kandreadou
 */
public class WikiMapiaStream extends Stream {

    public static String SOURCE = Sources.WIKIMAPIA;

    private Logger logger = Logger.getLogger(StreetViewStream.class);

    private String apiKey;

    @Override
    public void close() throws StreamException {
        logger.info("#Wikimapia : Close stream");
    }

    @Override
    public void open(Configuration config) throws StreamException {
        logger.info("#Wikimapia : Open stream");

        if (config == null) {
            logger.error("#Wikimapia : Config file is null.");
            return;
        }

        this.apiKey = config.getParameter(KEY);

        if (apiKey == null) {
            logger.error("#Wikimapia : Stream requires api key.");
            throw new StreamException("Stream requires api key");
        }

        Credentials credentials = new Credentials();
        credentials.setKey(apiKey);

        maxRequests = Integer.parseInt(config.getParameter(MAX_REQUESTS));
        timeWindow = Long.parseLong(config.getParameter(TIME_WINDOW));

        rateLimitsMonitor = new RateLimitsMonitor(maxRequests, timeWindow);
        try {
            retriever = new WikiMapiaRetriever(credentials);
        } catch (Exception e) {
            throw new StreamException(e);
        }
    }

    @Override
    public String getName() {
        return SOURCE;
    }
}
