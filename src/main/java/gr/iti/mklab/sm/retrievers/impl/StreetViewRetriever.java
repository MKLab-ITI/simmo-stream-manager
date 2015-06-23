package gr.iti.mklab.sm.retrievers.impl;

import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.util.Location;
import gr.iti.mklab.simmo.impl.Sources;
import gr.iti.mklab.sm.Credentials;
import gr.iti.mklab.sm.feeds.GeoFeed;
import gr.iti.mklab.sm.retrievers.GeoRetriever;
import gr.iti.mklab.sm.retrievers.Response;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;

/**
 * Class responsible for retrieving Google StreetView images
 *
 * @author kandreadou
 */
public class StreetViewRetriever extends GeoRetriever {

    private Logger logger = Logger.getLogger(StreetViewRetriever.class);
    private final static int MINIMUM_CONTENT_LENGTH = 15000;
    private String key;

    public StreetViewRetriever(Credentials credentials) throws Exception {
        super(credentials);

        if (credentials.getKey() == null) {
            logger.error("YouTube requires authentication.");
            throw new Exception("StreetView requires an API key.");
        }
        key = credentials.getKey();
    }

    @Override
    public Response retrieveGeoFeed(GeoFeed feed, Integer maxRequests) {
        Response response = new Response();
        List<Image> images = new ArrayList<>();
        int count = 0;

        for (double x = feed.get_lat_min(); x < feed.get_lat_max(); x += feed.get_density()) {
            for (double y = feed.get_lon_min(); y < feed.get_lon_max(); y += feed.get_density()) {
                try {

                    BigDecimal bd = new BigDecimal(x);
                    bd = bd.setScale(6, BigDecimal.ROUND_HALF_UP);
                    double lat = bd.doubleValue();
                    bd = new BigDecimal(y);
                    bd = bd.setScale(6, BigDecimal.ROUND_HALF_UP);
                    double lon = bd.doubleValue();
                    //System.out.println("Getting streetview image for " + lat + " " + lon);
                    URL myUrl = new URL("http://maps.googleapis.com/maps/api/streetview?size=640x640&location=" + lat + "," + lon + "&sensor=false&key=" + key);
                    URLConnection connection = myUrl.openConnection();
                    // This is to avoid empty images containing just a text "no image available"
                    if (connection.getContentLength() > MINIMUM_CONTENT_LENGTH) {
                        Image img = new Image();
                        img.setId(Sources.GOOGLE_STREETVIEW + '#' + myUrl.hashCode());
                        img.setUrl(myUrl.toString());
                        img.setSource(Sources.GOOGLE_STREETVIEW);
                        img.setWidth(640);
                        img.setHeight(640);
                        img.setLocation(new Location(lat, lon));
                        images.add(img);
                        //BufferedImage image = ImageIO.read(myUrl);
                        //ImageIO.write(image, "JPEG", new File(imageFolder + lat + lon + ".jpg"));
                        count++;
                        if (count > maxRequests) {
                            break;
                        }
                    }

                } catch (MalformedURLException ex) {
                    System.out.println(ex);
                } catch (IOException ioex) {
                    System.out.println(ioex);
                }
            }
        }
        response.setMedia(images);
        return response;
    }
}
