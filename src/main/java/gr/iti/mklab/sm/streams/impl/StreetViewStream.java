package gr.iti.mklab.sm.streams.impl;

import gr.iti.mklab.simmo.impl.Sources;
import gr.iti.mklab.sm.Configuration;
import gr.iti.mklab.sm.Credentials;
import gr.iti.mklab.sm.retrievers.impl.StreetViewRetriever;
import gr.iti.mklab.sm.streams.Stream;
import gr.iti.mklab.sm.streams.StreamException;
import gr.iti.mklab.sm.streams.monitors.RateLimitsMonitor;
import org.apache.log4j.Logger;

/**
 * Class responsible for setting up the connection to Google StreetView
 * for retrieving relevant StreetView content.
 *
 * @author kandreadou
 */
public class StreetViewStream extends Stream {

    public static String SOURCE = Sources.GOOGLE_STREETVIEW;

    private Logger logger = Logger.getLogger(StreetViewStream.class);

    private String apiKey;

    @Override
    public void close() throws StreamException {
        logger.info("#StreetView : Close stream");
    }

    @Override
    public void open(Configuration config) throws StreamException {
        logger.info("#StreetView : Open stream");

        if (config == null) {
            logger.error("#StreetView : Config file is null.");
            return;
        }

        this.apiKey = config.getParameter(KEY);

        if (apiKey == null) {
            logger.error("#StreetView : Stream requires api key.");
            throw new StreamException("Stream requires api key");
        }

        Credentials credentials = new Credentials();
        credentials.setKey(apiKey);

        maxRequests = Integer.parseInt(config.getParameter(MAX_REQUESTS));
        timeWindow = Long.parseLong(config.getParameter(TIME_WINDOW));

        rateLimitsMonitor = new RateLimitsMonitor(maxRequests, timeWindow);
        try {
            retriever = new StreetViewRetriever(credentials);
        } catch (Exception e) {
            throw new StreamException(e);
        }
    }

    @Override
    public String getName() {
        return SOURCE;
    }
}
