package gr.iti.mklab.manager.storages;


import gr.iti.mklab.simmo.documents.Post;

import java.io.IOException;


/**
 * Represents a storage for stream items
 *
 */
public interface Storage {

	/**
	 * Opens the store
	 * @throws IOException
	 */
	public boolean open();
	
	/**
	 * Stores a single stream update within store
	 * @param update
	 * 			Stream update to store
	 * @throws IOException
	 */
	public void store(Post post) throws IOException;
	
	
	/**
	 * Stores a single stream update within store
	 * @param update
	 * 			Stream update to store
	 * @throws IOException
	 */
	public void update(Post post) throws IOException;
	
	/**
	 * Delete a single stream update within store based on its id
	 * @param update
	 * 			Stream update to delete
	 * @return
	 *         Deletion status
	 * @throws IOException
	 */
	public boolean delete(String id) throws IOException;
	
	public boolean checkStatus();
	
	/**
	 * Close the store
	 */
	public void close();
	
	public String getStorageName();
	
}
