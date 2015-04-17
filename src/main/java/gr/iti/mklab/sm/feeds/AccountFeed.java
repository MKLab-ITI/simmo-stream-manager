package gr.iti.mklab.sm.feeds;

import java.util.Date;

public class AccountFeed extends Feed {
	
	private String username = null;
	
	public AccountFeed() {
		
	}
	
	public AccountFeed(String id, String username, Date since) {
		super(since);
		this.id = id;
		this.username = username;
	}

	public String getUsername() {
		return this.username;
	}
	
	public void setUsername(String username) {
		this.username = username;
	}

}
