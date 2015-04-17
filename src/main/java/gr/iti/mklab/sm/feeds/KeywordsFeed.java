package gr.iti.mklab.sm.feeds;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class KeywordsFeed extends Feed {
	
	private List<String> keywords = new ArrayList<String>();
	
	public KeywordsFeed() {
		
	}
	
	public KeywordsFeed(String id, String keyword, Date since) {
		super(since);
		this.keywords.add(keyword);
		this.id = id;
	}
	
	public KeywordsFeed(String id, List<String> keywords, Date since) {
		super(since);
		this.keywords.addAll(keywords);
		this.id = id;
	}
	
	public List<String> getKeywords() {
		return keywords;
	}
	
	public void setKeywords(List<String> keywords) {
		this.keywords.clear();
		this.keywords.addAll(keywords);
	}
	
	public void addKeywords(List<String> keywords) {
		this.keywords.addAll(keywords);
	}
	
	public void addKeyword(String keyword) {
		this.keywords.add(keyword);
	}
}
