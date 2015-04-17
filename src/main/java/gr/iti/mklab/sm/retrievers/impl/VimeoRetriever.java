package gr.iti.mklab.sm.retrievers.impl;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;

import gr.iti.mklab.simmo.core.UserAccount;
import gr.iti.mklab.sm.Credentials;
import gr.iti.mklab.sm.feeds.AccountFeed;
import gr.iti.mklab.sm.feeds.Feed;
import gr.iti.mklab.sm.feeds.GroupFeed;
import gr.iti.mklab.sm.feeds.KeywordsFeed;
import gr.iti.mklab.sm.retrievers.Response;
import gr.iti.mklab.sm.retrievers.SocialMediaRetriever;

/**
 * The retriever that implements the Vimeo simplified retriever 
 * @author manosetro
 */
public class VimeoRetriever extends SocialMediaRetriever {

	static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	static final JsonFactory JSON_FACTORY = new JacksonFactory();
	
	private HttpRequestFactory requestFactory;
	private String requestPrefix = "http://vimeo.com/api/v2/video/";
	
	public VimeoRetriever(Credentials credentials) {
		super(credentials);
		
		requestFactory = HTTP_TRANSPORT.createRequestFactory(
				new HttpRequestInitializer() {
					@Override
					public void initialize(HttpRequest request) {
						request.setParser(new JsonObjectParser(JSON_FACTORY));
					}
				});
	}
	
	/*
	public MediaItem getMediaItem(String id) {
		try {
			GenericUrl url = new GenericUrl(requestPrefix + id + ".json");
			HttpRequest request = requestFactory.buildGetRequest(url);
			HttpResponse response = request.execute();
			VimeoVideo.VimeoVideo[] videos = response.parseAs(VimeoVideo.VimeoVideo[].class);
			if(videos != null && videos.length>0) {
				MediaItem mediaItem = new VimeoVideo(videos[0]);
				return mediaItem;
			}
		} catch (Exception e) {
			//e.printStackTrace();
		}
		return null;	 	
	}
	*/
	
	@Override
	public Response retrieve(Feed feed) {
		return new Response();
	}

	@Override
	public Response retrieveKeywordsFeed(KeywordsFeed feed) throws Exception {
		return new Response();
	}
	
	@Override
	public Response retrieveAccountFeed(AccountFeed feed) throws Exception {
		return new Response();
	}

	@Override
	public UserAccount getStreamUser(String uid) {

		return null;
	}

	@Override
	public Response retrieveGroupFeed(GroupFeed feed) {
		return new Response();
	}

	@Override
	public Response retrieve(Feed feed, Integer maxRequests) {
		return null;
	}

	@Override
	public Response retrieveKeywordsFeed(KeywordsFeed feed, Integer maxRequests) throws Exception {
		return null;
	}

	@Override
	public Response retrieveAccountFeed(AccountFeed feed, Integer maxRequests) throws Exception {
		return null;
	}

	@Override
	public Response retrieveGroupFeed(GroupFeed feed, Integer maxRequests) {
		return null;
	}

}
