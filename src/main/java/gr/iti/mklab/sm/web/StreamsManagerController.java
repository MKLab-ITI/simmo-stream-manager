package gr.iti.mklab.sm.web;

import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import gr.iti.mklab.sm.StreamsManager;
import gr.iti.mklab.sm.feeds.GeoFeed;
import gr.iti.mklab.sm.feeds.KeywordsFeed;
import gr.iti.mklab.sm.streams.StreamException;
import gr.iti.mklab.sm.streams.StreamsManagerConfiguration;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.bind.annotation.ResponseBody;
import org.xml.sax.SAXException;

import javax.annotation.PreDestroy;
import javax.xml.parsers.ParserConfigurationException;

import java.io.File;
import java.io.IOException;
import java.util.Map;

@Controller
@RequestMapping("/api")
public class StreamsManagerController {

    private StreamsManager manager;
	private Thread thread;

    public StreamsManagerController() throws StreamException, IOException, SAXException, ParserConfigurationException {
        
    	ClassLoader classLoader = getClass().getClassLoader();
    	File streamConfigFile = new File(classLoader.getResource("streams.conf.xml").getFile());
    	
        StreamsManagerConfiguration config = StreamsManagerConfiguration.readFromFile(streamConfigFile);
        
        manager = new StreamsManager(config);
        manager.open();
        
        thread = new Thread(manager);
        thread.start();
    }

    @PreDestroy
    public void cleanUp() throws Exception {
        MorphiaManager.tearDown();
        if (manager != null) {
            manager.close();
        }
        try {
        	thread.interrupt();
        }
        catch(Exception e) {
        	e.printStackTrace();
        }
    }

    /**
     * ** keywords feed request
     * 	{
     * 		"keywords":["grexit"],
     * 		"id":"Flickr#1",
     * 		"since":"Jun 10, 2015 11:32:47 AM",
     * 		"source":"Flickr",
     * 		"label":"tFlickr"
     * 	}
     */
    @RequestMapping(value = "/feeds/addkeywords", method = RequestMethod.POST, produces = "application/json", consumes = "application/json")
    @ResponseBody
    public String addKeywordsFeed(@RequestBody KeywordsFeed feed) throws Exception {
        return manager.addFeed(feed);
    }

    /**
     * ** geo feed request
     * 	{
     * 		"_lon_min":2.282352,
     * 		"_lat_min":48.837379,
     * 		"_lon_max":2.394619,
     * 		"_lat_max":48.891358,
     * 		"_density":0.0,
     * 		"id":"Panoramio#1",
     * 		"source":"Panoramio",
     * 		"label":"tPanoramio"
     * 	}
     */
    @RequestMapping(value = "/feeds/addgeo", method = RequestMethod.POST, produces = "application/json", consumes = "application/json")
    @ResponseBody
    public String addGeoFeed(@RequestBody GeoFeed feed) throws Exception {
        return manager.addFeed(feed);
    }


    @RequestMapping(value = "/feeds/delete", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public String deleteFeed(@RequestParam String id) throws Exception {
        return manager.deleteFeed(id);
    }

    @RequestMapping(value = "/status", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public Map<String, Object> status() throws Exception {
    	Map<String, Object> status = manager.getStatus();
    	return status;
    }
}
