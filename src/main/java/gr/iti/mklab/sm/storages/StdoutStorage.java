package gr.iti.mklab.sm.storages;

import gr.iti.mklab.simmo.core.documents.Post;
import gr.iti.mklab.sm.Configuration;

import java.io.IOException;

public class StdoutStorage implements Storage {
	
	private String storageName = "StdOut";

	public StdoutStorage(Configuration config) {
		
	}

	@Override
	public void store(gr.iti.mklab.simmo.core.Object update) throws IOException {
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
