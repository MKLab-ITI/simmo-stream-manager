package gr.iti.mklab.sm.retrievers.impl;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.youtube.YouTube;
import com.google.api.services.youtube.model.*;
import gr.iti.mklab.simmo.core.UserAccount;
import gr.iti.mklab.simmo.core.documents.Post;
import gr.iti.mklab.simmo.impl.media.YoutubeVideo;
import gr.iti.mklab.simmo.impl.posts.YoutubePost;
import gr.iti.mklab.simmo.impl.users.YoutubeAccount;
import gr.iti.mklab.simmo.impl.users.YoutubeChannel;
import gr.iti.mklab.sm.Credentials;
import gr.iti.mklab.sm.feeds.AccountFeed;
import gr.iti.mklab.sm.feeds.GroupFeed;
import gr.iti.mklab.sm.feeds.KeywordsFeed;
import gr.iti.mklab.sm.retrievers.Response;
import gr.iti.mklab.sm.retrievers.SocialMediaRetriever;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class responsible for retrieving YouTube content based on keywords and YouTube users
 * The retrieval process takes place through Google API v3
 *
 * @author kandreadou
 */
public class YouTubeRetriever extends SocialMediaRetriever {

    /**
     * Define a global instance of the HTTP transport.
     */
    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

    /**
     * Define a global instance of the JSON factory.
     */
    public static final JsonFactory JSON_FACTORY = new JacksonFactory();
    /**
     * Define a global instance of the Logger
     */
    private static final Logger logger = Logger.getLogger(YouTubeRetriever.class);

    private static final long NUMBER_OF_VIDEOS_RETURNED = 25;
    private YouTube youtube;
    private final String API_KEY;

    public YouTubeRetriever(Credentials credentials) throws Exception {
        super(credentials);

        if (credentials.getClientId() == null || credentials.getKey() == null) {
            logger.error("YouTube requires authentication.");
            throw new Exception("YouTube requires authentication.");
        }
        API_KEY = credentials.getKey();

        youtube = new YouTube.Builder(HTTP_TRANSPORT, JSON_FACTORY, new HttpRequestInitializer() {
            public void initialize(HttpRequest request) throws IOException {
            }
        }).setApplicationName(credentials.getClientId()).build();

    }

    @Override
    public Response retrieveKeywordsFeed(KeywordsFeed feed, Integer maxRequests) throws Exception {

        // The total number of requests, summing up search, channel list, video list
        int numRequests = 0;
        // The total number of results, as returned by the search response
        int totalResults = 0;
        // The toal number of results of the search requests
        int resultsCounter = 0;
        String nextPageToken = null;
        Response response = new Response();
        List<YoutubeVideo> videos = new ArrayList<>();

        List<String> keywords = feed.getKeywords();

        if (keywords == null || keywords.isEmpty()) {
            logger.error("#YouTube : No keywords feed");
            return response;
        }

        String tags = "";
        for (String key : keywords) {
            String[] words = key.split(" ");
            for (String word : words) {
                if (!tags.contains(word) && word.length() > 1)
                    tags += word.toLowerCase() + " ";
            }
        }

        //one call - 25 results
        if (tags.equals(""))
            return response;

        while (numRequests <= maxRequests && resultsCounter<=totalResults) {
            // Define the API request for retrieving search results.
            YouTube.Search.List search = youtube.search().list("id,snippet");
            search.setKey(API_KEY);
            search.setQ(tags);

            // Restrict the search results to only include videos. See:
            // https://developers.google.com/youtube/v3/docs/search/list#type
            search.setType("video");

            // To increase efficiency, only retrieve the fields that the
            // application uses.
            search.setFields("items(id/videoId,snippet/channelId),pageInfo,nextPageToken");
            search.setMaxResults(NUMBER_OF_VIDEOS_RETURNED);
            search.setPageToken(nextPageToken);
            String ids = null;
            String channelids = null;
            // Call the API and print results.
            SearchListResponse searchResponse = search.execute();
            List<SearchResult> searchResultList = searchResponse.getItems();
            numRequests+=searchResultList.size();
            resultsCounter +=searchResultList.size();
            nextPageToken = searchResponse.getNextPageToken();
            totalResults = searchResponse.getPageInfo().getTotalResults();
            for (SearchResult sr : searchResultList) {
                ids += sr.getId().getVideoId() + ",";
                channelids += sr.getSnippet().getChannelId() + ",";
            }

            Map<String, Channel> users = new HashMap<>();
            YouTube.Channels.List channelsSearch = youtube.channels().list("id,snippet,statistics");
            channelsSearch.setKey(API_KEY);
            channelsSearch.setId(channelids);
            ChannelListResponse channelListResponse = channelsSearch.execute();
            numRequests+=channelListResponse.size();
            for (Channel c : channelListResponse.getItems()) {
                users.put(c.getId(), c);
            }

            YouTube.Videos.List videoSearch = youtube.videos().list("id,snippet,statistics");
            videoSearch.setKey(API_KEY);
            videoSearch.setId(ids);
            VideoListResponse videoListResponse = videoSearch.execute();
            numRequests+=videoListResponse.size();
            for (Video v : videoListResponse.getItems()) {
                videos.add(new YoutubeVideo(v, users.get(v.getSnippet().getChannelId())));
            }

        }
        response.setMedia(videos);
        return response;
    }

    @Override
    public Response retrieveAccountFeed(AccountFeed feed, Integer maxRequests) throws Exception {
        return null;
    }

    @Override
    public Response retrieveGroupFeed(GroupFeed feed, Integer maxRequests) {
        return null;
    }

    @Override
    public UserAccount getStreamUser(String uid) {
        return null;
    }
}
