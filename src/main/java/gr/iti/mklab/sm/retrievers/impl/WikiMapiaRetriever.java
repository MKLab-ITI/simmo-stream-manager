package gr.iti.mklab.sm.retrievers.impl;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Key;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.util.Location;
import gr.iti.mklab.simmo.impl.Sources;
import gr.iti.mklab.sm.Credentials;
import gr.iti.mklab.sm.feeds.GeoFeed;
import gr.iti.mklab.sm.retrievers.GeoRetriever;
import gr.iti.mklab.sm.retrievers.Response;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Class responsible for retrieving WikiMapia POIs
 *
 * @author kandreadou
 */
public class WikiMapiaRetriever extends GeoRetriever {

    private Logger logger = Logger.getLogger(WikiMapiaRetriever.class);
    /**
     * The maximum number of photos one can retrieve in one query as per API definition
     */
    private static final int MAX_RESULTS_PER_QUERY = 50;
    static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    static final JsonFactory JSON_FACTORY = new JacksonFactory();

    private HttpRequestFactory requestFactory;
    private String key;

    public WikiMapiaRetriever(Credentials credentials) throws Exception {
        super(credentials);

        if (credentials.getKey() == null) {
            logger.error("Wikimapia requires an API key.");
            throw new Exception("Wikimapia requires an API key.");
        }
        key = credentials.getKey();
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
        int page = 1;
        int found = 0;
        Response res = new Response();
        List<Image> resultArray = new ArrayList<>();
        while (page < maxRequests && current <= found) {
            String req = "http://api.wikimapia.org/?key=%s&function=place.getbyarea&coordsby=bbox&bbox==%f,%f,%f,%f&format=json&data_blocks=photos,location&page=%d&count=%d";
            GenericUrl requestUrl = new GenericUrl(String.format(req, key, feed.get_lon_min(), feed.get_lat_min(), feed.get_lon_max(), feed.get_lat_max(), page, MAX_RESULTS_PER_QUERY));
            logger.info(requestUrl.toString());
            HttpRequest request;
            try {
                request = requestFactory.buildGetRequest(requestUrl);
                HttpResponse response = request.execute();
                WikiMapiaContainer result = response.parseAs(WikiMapiaContainer.class);
                if (result == null)
                    continue;
                found = result.found;
                if (result.places == null)
                    continue;
                for (WikiMapiaContainer.Place place : result.places)
                    for (WikiMapiaContainer.Place.Photo photo : place.photos) {
                        //logger.info(photo.big_url);
                        Image im = new Image();
                        im.setId(Sources.WIKIMAPIA + '#' + place.id + '#' + photo.id);
                        im.setCrawlDate(new Date());
                        im.setSource(Sources.WIKIMAPIA);
                        im.setUrl(photo.big_url);
                        im.setThumbnail(photo.thumbnail_url);
                        if (place.description != null)
                            im.setDescription(place.description);
                        if (place.title != null)
                            im.setTitle(place.title);
                        if (place.wikipedia != null)
                            im.setWebPageUrl(place.wikipedia);
                        if (place.location != null) {
                            Location loc = new Location(place.location.lat, place.location.lon);
                            loc.setCountry(place.location.country);
                            loc.setCity(place.location.place);
                            if (place.location.street != null) {
                                if (place.location.housenumber == null)
                                    loc.setAdress(place.location.street);
                                else
                                    loc.setAdress(place.location.street + " " + place.location.housenumber);
                            }
                            im.setLocation(loc);
                        }
                        if (place.tags != null) {
                            for (WikiMapiaContainer.Place.Tag tag : place.tags)
                                im.addTag(tag.title);
                        }
                        resultArray.add(im);
                    }

            } catch (Exception e) {
                logger.error(e + " " + e.getMessage());
            }
            page++;
            current += MAX_RESULTS_PER_QUERY;
        }
        res.setMedia(resultArray);
        return res;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class WikiMapiaContainer {

        @Key
        public Place[] places;
        @Key
        public int found;

        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class Place {
            @Key
            public long id;
            @Key
            public Tag[] tags;
            @Key
            public String description, wikipedia, title;
            @Key
            public Photo[] photos;
            @Key
            public int distance;

            @Key
            public Location location;

            public static class Tag {
                @Key
                public long id;
                @Key
                public String title;
            }

            @JsonIgnoreProperties(ignoreUnknown = true)
            public static class Photo {
                @Key
                public long id;
                @Key
                public String time_str, big_url, thumbnail_url;
            }

            @JsonIgnoreProperties(ignoreUnknown = true)
            public static class Location {
                @Key
                public double lat, lon;
                @Key
                public String country, state, place, street, housenumber;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        double lat1 = 48.837379;
        double lon1 = 2.282352;
        double lat2 = 48.891358;
        double lon2 = 2.394619;
        double density = 0.02;
        GeoFeed feed = new GeoFeed(lon1, lat1, lon2, lat2, density);
        Credentials c = new Credentials("example", null, null, null);
        WikiMapiaRetriever r = new WikiMapiaRetriever(c);
        Response s = r.retrieveGeoFeed(feed, 5);
    }
}
