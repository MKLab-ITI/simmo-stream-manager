package gr.iti.mklab.sm.retrievers;

import gr.iti.mklab.simmo.core.documents.Post;
import gr.iti.mklab.simmo.core.items.Image;

import java.util.ArrayList;
import java.util.List;

public class Response {
	
	private List<Post> items = new ArrayList<Post>();
    private List<Image> images = new ArrayList<>();

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

    public void setImages(List<Image> images) {
        this.images.addAll(images);
    }

    public List<Image> getImages() {
        return images;
    }
}
