package gr.iti.mklab.sm.management;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import gr.iti.mklab.sm.Configuration;
import gr.iti.mklab.sm.filters.ItemFilter;

import org.apache.log4j.Logger;

import gr.iti.mklab.sm.storages.Storage;
import gr.iti.mklab.sm.streams.StreamException;
import gr.iti.mklab.sm.streams.StreamsManagerConfiguration;

/**
 * @brief  Thread-safe class for managing the storage of items to databases 
 * The storage may be accomplished using multiple consumer-threads.
 * 
 * @author Manos Schinas 
 * @email  manosetro@iti.gr
 *
 */
public class StorageHandler {
	
	public final Logger logger = Logger.getLogger(StorageHandler.class);
	
	// Internal queue used as a buffer of incoming items 
	private BlockingQueue<gr.iti.mklab.simmo.core.Object> queue = new LinkedBlockingDeque<gr.iti.mklab.simmo.core.Object>();
	
	private int numberOfConsumers = 16;
	private List<Consumer> consumers = new ArrayList<Consumer>(numberOfConsumers);
	
	private List<Storage> storages = new ArrayList<Storage>();
	private List<ItemFilter> filters = new ArrayList<ItemFilter>();
	
	public enum StorageHandlerState {
		OPEN, CLOSE
	}
	
	private StorageHandlerState state = StorageHandlerState.CLOSE;
	
	public StorageHandler(StreamsManagerConfiguration config) {
		try {	
			
			createFilters(config);
			
			initializeStorageHandler(config);	
			state = StorageHandlerState.OPEN;
			
		} catch (StreamException e) {
			logger.error("Error during storage handler initialization: " + e.getMessage());
		}
	}
	
	public StorageHandlerState getState() {
		return state;
	}
	
	/**
	 * Starts the consumer threads responsible for storing
	 * items to the database.
	 */
	public void start() {
		for(int i=0; i<numberOfConsumers; i++) {
			Consumer consumer = new Consumer(queue, storages, filters);
			consumers.add(consumer);
		}
		
		for(Consumer consumer : consumers) {
			consumer.start();
		}
	}

	public void handle(gr.iti.mklab.simmo.core.Object item) {
		try {
			queue.add(item);
		}
		catch(Exception e) {
			logger.error(e);
		}
	}

	public void handle(gr.iti.mklab.simmo.core.Object[] posts) {
		for (gr.iti.mklab.simmo.core.Object item : posts) {
			handle(item);
		}
	}
	
	
	public void delete(String id) {
		for(Storage storage : storages) {
			try {
				storage.delete(id);
			} catch (IOException e) {
				logger.error(e);
			}	
		}
	}
	
	/**
	 * Initializes the databases that are going to be used in the service
	 * @param config
	 * @return
	 * @throws StreamException
	 */
	private void initializeStorageHandler(StreamsManagerConfiguration config) throws StreamException {
		for (String storageId : config.getStorageIds()) {
			Configuration storageConfig = config.getStorageConfig(storageId);
			try {
				String storageClass = storageConfig.getParameter(Configuration.CLASS_PATH);
				Constructor<?> constructor = Class.forName(storageClass).getConstructor(Configuration.class);
				Storage storageInstance = (Storage) constructor.newInstance(storageConfig);
				if(storageInstance.open()) {
					storages.add(storageInstance);
				}
		
			} catch (Exception e) {
				throw new StreamException("Error during storage initialization", e);
			}
		}
	}
	
	private void createFilters(StreamsManagerConfiguration config) throws StreamException {
		for (String filterId : config.getFilterIds()) {
			try {
				logger.info("Initialize filter " + filterId);
				Configuration fconfig = config.getFilterConfig(filterId);
				String className = fconfig.getParameter(Configuration.CLASS_PATH);
				Constructor<?> constructor = Class.forName(className).getConstructor(Configuration.class);
				ItemFilter filterInstance = (ItemFilter) constructor.newInstance(fconfig);
			
				filters.add(filterInstance);
			}
			catch(Exception e) {
				logger.error("Error during filter " + filterId + "initialization", e);
			}
		}
	}
	/**
	 * Stops all consumer threads and all the databases used
	 */
	public void stop() {
		state = StorageHandlerState.CLOSE;
		for(Consumer consumer : consumers) {
			consumer.die();
		}
		
		for(Storage storage : storages) {
			storage.close();
		}
	}
}