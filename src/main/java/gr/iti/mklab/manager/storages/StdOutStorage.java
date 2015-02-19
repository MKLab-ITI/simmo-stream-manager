package gr.iti.mklab.manager.storages;

import gr.iti.mklab.manager.config.Configuration;
import gr.iti.mklab.simmo.documents.Post;

import java.io.IOException;

public class StdOutStorage implements Storage {
	
	private String storageName = "StdOut";

	public StdOutStorage(Configuration config) {
		
	}


	@Override
	public void store(Post post) throws IOException {
		System.out.println(post.toString());	
	}

	@Override
	public boolean delete(String id) throws IOException {
		System.out.println("{ delete : " + id + "}");	
		return false;
	}

	@Override
	public boolean open(){
		return true;
	}

	@Override
	public void close() {
		
	}
	
	@Override
	public boolean checkStatus() {
		return true;
	}

	@Override
	public void update(Post post) throws IOException {
		System.out.println(post.toString());
	}
	
	@Override
	public String getStorageName(){
		return this.storageName;
	}

}
