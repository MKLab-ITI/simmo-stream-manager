package gr.iti.mklab.sm.retrievers.impl;

import com.google.api.client.http.*;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Key;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.impl.media.PanoramioImage;
import gr.iti.mklab.sm.feeds.GeoFeed;
import gr.iti.mklab.sm.retrievers.GeoRetriever;
import gr.iti.mklab.sm.retrievers.Response;

import java.util.ArrayList;
import java.util.List;


/**
 * A retriever for Panoramio
 *
 * @author kandreadou
 */
public class PanoramioRetriever extends GeoRetriever {

    /** The maximum number of photos one can retrieve in one query as per API definition */
    private static final int MAX_PHOTOS_PER_QUERY = 100;
    static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    static final JsonFactory JSON_FACTORY = new JacksonFactory();

    private HttpRequestFactory requestFactory;

    public PanoramioRetriever() {
        requestFactory = HTTP_TRANSPORT.createRequestFactory(
                new HttpRequestInitializer() {
                    @Override
                    public void initialize(HttpRequest request) {
                        request.setParser(new JsonObjectParser(JSON_FACTORY));
                    }
                });
    }

    @Override
    public Response retrieveGeoFeed(GeoFeed feed, Integer maxRequests) {
        int current = 0;
        Response res = new Response();
        List<Image> resultArray = new ArrayList<>();
        while (current < maxRequests) {
            String req = "http://www.panoramio.com/map/get_panoramas.php?set=public&from=%d&to=%d&minx=%f&miny=%f&maxx=%f&maxy=%f&size=medium&mapfilter=true";
            GenericUrl requestUrl = new GenericUrl(String.format(req, current, current + MAX_PHOTOS_PER_QUERY, feed.get_lat_min(), feed.get_lon_min(), feed.get_lon_max(), feed.get_lat_max()));

            HttpRequest request;
            try {
                //System.out.println(requestUrl.toString());
                request = requestFactory.buildGetRequest(requestUrl);
                HttpResponse response = request.execute();
                PanoramioContainer result = response.parseAs(PanoramioContainer.class);
                if (result != null) {
                    for (PanoramioImage.PanoramioItem item : result.photos)
                        resultArray.add(new PanoramioImage(item));
                }
            } catch (Exception e) {
                System.out.println(e);
            }
            current += MAX_PHOTOS_PER_QUERY;
        }
        res.setMedia(resultArray);
        return res;
    }

    public static class PanoramioContainer {

        @Key
        public int count;
        @Key
        public boolean has_more;
        @Key
        public MapLocation map_location;
        @Key
        public PanoramioImage.PanoramioItem[] photos;

        public static class MapLocation {
            public double lat;
            public double lon;
            public int panoramio_zoom;
        }
    }

    public static void main(String[] args) {
        double lat1 = 48.837379;
        double lon1 = 2.282352;
        double lat2 = 48.891358;
        double lon2 = 2.394619;
        double density = 0.02;
        GeoFeed feed = new GeoFeed(lon1, lat1, lon2, lat2, density);
        PanoramioRetriever r = new PanoramioRetriever();
        Response s = r.retrieveGeoFeed(feed, 5);
    }
}
