package gr.iti.mklab.sm.streams.impl;

import gr.iti.mklab.simmo.impl.Sources;
import gr.iti.mklab.sm.Configuration;
import gr.iti.mklab.sm.retrievers.impl.PanoramioRetriever;
import gr.iti.mklab.sm.streams.Stream;
import gr.iti.mklab.sm.streams.StreamException;
import gr.iti.mklab.sm.streams.monitors.RateLimitsMonitor;
import org.apache.log4j.Logger;

/**
 * Class responsible for setting up the connection to Panoramio
 * for retrieving relevant Panoramio content.
 *
 * @author kandreadou
 */
public class PanoramioStream extends Stream {

    public static String SOURCE = Sources.PANORAMIO;

    private Logger logger = Logger.getLogger(PanoramioStream.class);


    @Override
    public void close() throws StreamException {
        logger.info("#Panoramio : Close stream");
    }

    @Override
    public void open(Configuration config) throws StreamException {
        logger.info("#Panoramio : Open stream");

        if (config == null) {
            logger.error("#Panoramio : Config file is null.");
            return;
        }

        maxRequests = Integer.parseInt(config.getParameter(MAX_REQUESTS));
        timeWindow = Long.parseLong(config.getParameter(TIME_WINDOW));

        rateLimitsMonitor = new RateLimitsMonitor(maxRequests, timeWindow);
        try {
            retriever = new PanoramioRetriever();
        } catch (Exception e) {
            throw new StreamException(e);
        }
    }

    @Override
    public String getName() {
        return SOURCE;
    }
}
