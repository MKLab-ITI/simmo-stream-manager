package gr.iti.mklab.manager.storages;

import gr.iti.mklab.manager.config.Configuration;
import gr.iti.mklab.simmo.documents.Post;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;


/**
 * Class for storing items to a flat file
 * @author manosetro
 * @email  manosetro@iti.gr
 *
 */
public class FlatFileStorage implements Storage {	
	
	private static String NAME = "name";
	private static String STORAGE_FILE = "file";
	
	private String storageName = "FlatFile";
	private String fileToStore;
	
	private File storageDirectory;
	private PrintWriter out = null;
	
	long items = 0;
	
	public FlatFileStorage(Configuration config) {
		this.fileToStore = config.getParameter(FlatFileStorage.NAME);
		
		String storage = config.getParameter(FlatFileStorage.STORAGE_FILE);
		this.storageDirectory = new File(storage);
	}
	
	
	public FlatFileStorage(String storageDirectory) {
		this.storageDirectory = new File(storageDirectory);
		this.fileToStore = "/items.";
	}
	
	@Override
	public void store(Post post) {
		items++;
		if (out != null) {
			out.println(post.getId() + "\t" + post.getCreationDate() + "\t" 
					+ post.getTitle().replaceAll("[\t\n]", " "));
			out.flush();
			
		}
		if(items%1000==0) {
			open();	
		}
	
	}

	
	@Override
	public boolean delete(String id) throws IOException {
		// cannot delete rows from flat files
		return false;
	}
	

	@Override
	public boolean open(){
		File storeFile = new File(storageDirectory, fileToStore+System.currentTimeMillis());
		try {
			out = new PrintWriter(new BufferedWriter(new FileWriter(storeFile, false)));
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	@Override
	public void close() {
		out.close();
	}

	@Override
	public void update(Post post) throws IOException {
		store(post);
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
