package gr.iti.mklab.simmo.sfc.storages;


import gr.iti.mklab.framework.Configuration;
import gr.iti.mklab.simmo.documents.Post;

import java.io.IOException;

public class StdoutStorage implements Storage {
	
	private String storageName = "StdOut";

	public StdoutStorage(Configuration config) {
		
	}


	@Override
	public void store(Post update) throws IOException {
		System.out.println(update.toString());	
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
	public String getStorageName(){
		return this.storageName;
	}

}
