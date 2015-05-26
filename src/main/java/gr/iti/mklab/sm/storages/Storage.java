package gr.iti.mklab.sm.storages;


import java.io.IOException;


/**
 * Represents a storage for stream items
 */
public interface Storage {

    /**
     * Opens the store
     *
     * @throws IOException
     */
    public boolean open();

    /**
     * Stores a single stream update within store
     *
     * @throws IOException
     */
    public void store(gr.iti.mklab.simmo.core.Object object) throws IOException;


    /**
     * Stores a single stream update within store
     * @param update
     * 			Stream update to store
     * @throws IOException
     */
    //public void update(Item update) throws IOException;

    /**
     * Delete a single stream update within store based on its id
     *
     * @return Deletion status
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
