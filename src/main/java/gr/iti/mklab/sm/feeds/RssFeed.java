package gr.iti.mklab.sm.feeds;

import java.util.Date;

public class RssFeed extends Feed {
	
	private String url = null;
	
	private String network = null;
	
	public RssFeed() {
		
	}

	public RssFeed(String id, String url, Date since) {
		super(since);
		this.url = url;
		this.id = id;
	}

	public String getURL() {
		return this.url;
	}
	
	public void setURL(String url) {
		this.url = url;
	}
	
	public String getNetwork() {
		return this.network;
	}
	
	public void setNetwork(String network) {
		this.network = network;
	}
}
