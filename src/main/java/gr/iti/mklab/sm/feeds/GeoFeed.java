package gr.iti.mklab.sm.feeds;

/**
 * A geo-based Feed.
 * Retrieves geotagged content from the available sources given the
 * geographic bounding box provided by the user.
 *
 * @author kandreadou
 */
public class GeoFeed extends Feed {

    private double _lon_min, _lat_min, _lon_max, _lat_max, _density;

    /**
     * A GeoFeed constructor
     *
     * @param lon_min , the minimum longitude
     * @param lat_min , the minimum latitude
     * @param lon_max , the maximum longitude
     * @param lat_max , the maximum latitude
     */
    public GeoFeed(double lon_min, double lat_min, double lon_max, double lat_max, double density) {
        _lon_min = lon_min;
        _lat_min = lat_min;
        _lon_max = lon_max;
        _lat_max = lat_max;
        _density = density;
    }

    public GeoFeed(double lon_min, double lat_min, double lon_max, double lat_max) {
        _lon_min = lon_min;
        _lat_min = lat_min;
        _lon_max = lon_max;
        _lat_max = lat_max;
    }

    public double get_lon_min() {
        return _lon_min;
    }

    public double get_lat_min() {
        return _lat_min;
    }

    public double get_lon_max() {
        return _lon_max;
    }

    public double get_lat_max() {
        return _lat_max;
    }

    public double get_density() {
        return _density;
    }


}
