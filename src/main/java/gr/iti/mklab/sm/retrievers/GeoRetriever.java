package gr.iti.mklab.sm.retrievers;

import gr.iti.mklab.sm.Credentials;
import gr.iti.mklab.sm.feeds.*;

/**
 * The interface for retrieving geo-based content - Currently the
 * supported geo-based sources are:
 * Google StreetView
 *
 * @author kandreadou
 */
public abstract class GeoRetriever implements Retriever {

    public GeoRetriever(){}

    public GeoRetriever(Credentials credentials) {
    }

    /**
     * Retrieves a geolocation-based feed which collects geotagged content
     * associated with the bounding box provided by the user
     *
     * @param feed         , the GeoFeed
     * @param maxRequests, the maximum number of requests
     * @return a Response
     * @throws Exception
     */
    public abstract Response retrieveGeoFeed(GeoFeed feed, Integer maxRequests);

    @Override
    public Response retrieve(Feed feed) throws Exception {
        return retrieve(feed, 1);
    }

    @Override
    public Response retrieve(Feed feed, Integer requests) {

        if (GeoFeed.class.isInstance(feed)) {
            GeoFeed geoFeed = (GeoFeed) feed;
            return retrieveGeoFeed(geoFeed, requests);
        }

        return new Response();
    }

}
