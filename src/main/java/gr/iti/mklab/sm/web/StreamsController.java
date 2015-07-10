package gr.iti.mklab.sm.web;

import com.google.gson.Gson;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import gr.iti.mklab.sm.StreamsManager;
import gr.iti.mklab.sm.feeds.KeywordsFeed;
import gr.iti.mklab.sm.streams.StreamException;
import gr.iti.mklab.sm.streams.StreamsManagerConfiguration;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.xml.sax.SAXException;

import javax.annotation.PreDestroy;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

@Controller
@RequestMapping("/sm")
public class StreamsController {

    private StreamsManager manager;

    public StreamsController() throws StreamException, IOException, SAXException, ParserConfigurationException {
        File streamConfigFile = new File("/home/kandreadou/mklab/streams.conf.xml");
        StreamsManagerConfiguration config = StreamsManagerConfiguration.readFromFile(streamConfigFile);
        manager = new StreamsManager(config);
        manager.open();
        Thread thread = new Thread(manager);
        thread.start();
    }

    @PreDestroy
    public void cleanUp() throws Exception {
        MorphiaManager.tearDown();
        if (manager != null)
            manager.close();
    }

    /**
     * {"keywords":["grexit"],"id":"Flickr#1","since":"Jun 10, 2015 11:32:47 AM","source":"Flickr","label":"tFlickr"}
     */
    @RequestMapping(value = "/feeds/add", method = RequestMethod.POST, produces = "application/json", consumes = "application/json")
    @ResponseBody
    public String addFeed(@RequestBody KeywordsFeed feed) throws Exception {
        //TODO: capped collection
        //trigger FeedMonitor: does it need triggering?
        return manager.addFeed(feed);
    }

    @RequestMapping(value = "/feeds/delete", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public String deleteFeed(@RequestParam String id) throws Exception {

        return manager.deleteFeed(id);
    }

    public static void main(String[] args) {
        Set<String> keywords = new HashSet<>();
        keywords.add("grexit");
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, -1);
        KeywordsFeed flickr = new KeywordsFeed();
        flickr.addKeywords(new ArrayList(keywords));
        flickr.setId("Flickr#1");
        flickr.setSinceDate(cal.getTime());
        flickr.setSource("Flickr");
        flickr.setLabel("tFlickr");
        Gson gson = new Gson();
        System.out.println(gson.toJson(flickr));
    }
}
