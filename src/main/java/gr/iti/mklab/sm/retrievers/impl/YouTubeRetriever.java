package gr.iti.mklab.sm.retrievers.impl;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Joiner;
import com.google.api.services.youtube.YouTube;
import com.google.api.services.youtube.model.*;

import gr.iti.mklab.simmo.core.UserAccount;
import gr.iti.mklab.simmo.core.documents.Post;
import gr.iti.mklab.simmo.core.items.Media;
import gr.iti.mklab.simmo.impl.media.YoutubeVideo;
import gr.iti.mklab.sm.Credentials;
import gr.iti.mklab.sm.feeds.AccountFeed;
import gr.iti.mklab.sm.feeds.GroupFeed;
import gr.iti.mklab.sm.feeds.KeywordsFeed;
import gr.iti.mklab.sm.retrievers.Response;
import gr.iti.mklab.sm.retrievers.SocialMediaRetriever;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    private static final long NUMBER_OF_RESULTS_RETURNED = 20;
    
    private YouTube youtube;
    private final String apiKey;

    public YouTubeRetriever(Credentials credentials) throws Exception {
        super(credentials);

        if (credentials.getClientId() == null || credentials.getKey() == null) {
            logger.error("YouTube requires authentication.");
            throw new Exception("YouTube requires authentication.");
        }
        apiKey = credentials.getKey();

        youtube = new YouTube.Builder(
        		HTTP_TRANSPORT, 
        		JSON_FACTORY, 
        		new HttpRequestInitializer() {
        			public void initialize(HttpRequest request) throws IOException {
        			
        			}
        		}
        	).setApplicationName(credentials.getClientId()).build();
    }

    @Override
    public Response retrieveKeywordsFeed(KeywordsFeed feed, Integer maxRequests) throws Exception {

        // The total number of requests, summing up search, channel list, video list
        int numberOfRequests = 0;

        Date sinceDate = feed.getSinceDate();
        String label = feed.getLabel();
        
        Response response = new Response();
        List<Post> posts = new ArrayList<Post>();
        List<Media> media = new ArrayList<Media>();
        
		// Define the API request for retrieving search results.
        YouTube.Search.List search = youtube.search()
        		.list("id");
        search.setKey(apiKey);
        
        List<String> keywords = feed.getKeywords();
		if(keywords == null || keywords.isEmpty()) {
			logger.error("#Youtube : No keywords feed");
			response = getResponse(posts, media, numberOfRequests);
			return response;
		}
		
		String textQuery = StringUtils.join(keywords, " OR ");
		if(textQuery.equals("")) {
			logger.error("Text Query is empty.");
			response = getResponse(posts, media, numberOfRequests);
			return response;
		}
        search.setQ(textQuery);
        search.setType("video");
        search.setMaxResults(NUMBER_OF_RESULTS_RETURNED);
        search.setOrder("date");
        
        Set<String> uids = new HashSet<String>();
        boolean sinceDateReached = false;
        String nextPageToken = null;
        while(true) {
        	try {
        		if(nextPageToken != null) {
        			search.setPageToken(nextPageToken);
        		}
        	
        		SearchListResponse searchResponse = search.execute();
        		numberOfRequests++;

        		List<SearchResult> searchResultList = searchResponse.getItems();
        		if (searchResultList != null) {
    			 
        			List<String> videoIds = new ArrayList<String>();
        			for (SearchResult searchResult : searchResultList) {         			
        				if(searchResult.getId().getVideoId() != null) {
        					videoIds.add(searchResult.getId().getVideoId());
        				}
        			}
        			Joiner stringJoiner = Joiner.on(',');
        			String videoId = stringJoiner.join(videoIds);
        			logger.info("Videos: " + videoId);
        			
        			YouTube.Videos.List listVideosRequest = youtube.videos().list("snippet,statistics,recordingDetails,player");
        			listVideosRequest.setId(videoId);
        			listVideosRequest.setMaxResults(NUMBER_OF_RESULTS_RETURNED);
        			listVideosRequest.setKey(apiKey);

        			VideoListResponse listResponse = listVideosRequest.execute();
        			numberOfRequests++;
            
        			List<Video> videoList = listResponse.getItems();
        			if (videoList != null) {
        				for(Video video : videoList) {
        					uids.add(video.getSnippet().getChannelId());
            			
        					YoutubeVideo yv = new YoutubeVideo(video);
        					if(yv.getCreationDate().before(sinceDate)) {
        						sinceDateReached = true;
        						break;
        					}
            			
        					if(label != null) {
        						yv.addLabel(label);
        					}
        					
        					media.add(yv);
        				}
        			}
        		}

        		nextPageToken = searchResponse.getNextPageToken();
        		if(nextPageToken == null) {
        			logger.info("Stop retriever. There is no more pages to fetch for query " + textQuery);
        			break;
        		}
        		
        	} catch (GoogleJsonResponseException e) {
				logger.error("There was a service error: " + e.getDetails().getCode() + " : " + e.getDetails().getMessage(), e);
				break;
			} catch (IOException e) {
				logger.error("There was an IO error: " + e.getCause() + " : " + e.getMessage(), e);
				break;
			} catch (Exception e) {
				e.printStackTrace();
				logger.error(e);
				break;
			}
        	
        	if(numberOfRequests >= maxRequests) {
        		logger.info("Stop retriever. Number of requests (" + numberOfRequests + ") has reached for " + textQuery);
				break;
			}
        	
			if(sinceDateReached) {
				logger.info("Stop retriever. Since date " + sinceDate + " reached for query " + textQuery);
				break;
			}
        }
        
        response = getResponse(posts, media, numberOfRequests);
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
