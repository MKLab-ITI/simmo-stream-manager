package gr.iti.mklab.sm.retrievers;

import gr.iti.mklab.simmo.documents.Post;

import java.util.ArrayList;
import java.util.List;

public class Response {
	
	private List<Post> items = new ArrayList<Post>();
	private int requests = 0;
	
	public void setPosts(List<Post> items) {
		this.items.addAll(items);
	}
	
	public List<Post> getPosts() {
		return items;
	}
	
	public int getNumberOfPosts() {
		return items.size();
	}
	
	public int getRequests() {
		return requests;
	}
	
	public void setRequests(int requests) {
		this.requests = requests;
	}
}
