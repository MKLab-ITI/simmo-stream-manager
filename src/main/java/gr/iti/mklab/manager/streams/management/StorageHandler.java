package gr.iti.mklab.manager.streams.management;

import gr.iti.mklab.manager.config.Configuration;
import gr.iti.mklab.manager.config.StreamManagerConfiguration;
import gr.iti.mklab.manager.storages.Consumer;
import gr.iti.mklab.manager.storages.Storage;
import gr.iti.mklab.simmo.documents.Post;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;


/**
 * @brief  Thread-safe class for managing the storage of items to databases 
 * The storage may be accomplished using multiple consumer-threads.
 * 
 * @author ailiakop
 * @email  ailiakop@iti.gr
 *
 */
public class StorageHandler {
	
	public final Logger logger = Logger.getLogger(StorageHandler.class);
	
	private BlockingQueue<Post> queue = new LinkedBlockingDeque<Post>();
	
	private Integer numberOfConsumers = 16;
	private List<Consumer> consumers;
	
	private List<Storage> storages = new ArrayList<Storage>();
	
	enum StorageHandlerState {
		OPEN, CLOSE
	}
	
	private StorageHandlerState state = StorageHandlerState.CLOSE;
	
	public StorageHandler(StreamManagerConfiguration config) {
		try {	
			state = StorageHandlerState.OPEN;
		
			consumers = new ArrayList<Consumer>(numberOfConsumers);

			initStorages(config);	
		} catch (Exception e) {
			logger.error(e);
		}
		
	}
	

	
	public StorageHandlerState getState() {
		return state;
	}
	
	public  List<Storage> getStorages() {
		return storages;
	}
	
	/**
	 * Starts the consumer threads responsible for storing
	 * items to the database.
	 */
	public void start() {
		
		for(int i=0; i<numberOfConsumers; i++) {
			Consumer consumer = new Consumer(queue, storages);
			consumer.setName("Consumer_" + i);
			consumers.add(consumer);
		}
		
		for(Consumer consumer : consumers) {
			consumer.start();
		}
	}

	public void update(Post post) {
		try {
			queue.add(post);
		}
		catch(Exception e) {
			logger.error(e);
		}
	}

	public void updates(Post[] posts) {
		for (Post post : posts) {
			update(post);
		}
	}
	
	
	public void delete(Post post) {
		queue.add(post);	
	}
	
	/**
	 * Initializes the databases that are going to be used in the service
	 * @param config
	 * @return
	 * @throws StreamException
	 */
	private void initStorages(StreamManagerConfiguration config) throws Exception {
		for (String storageId : config.getStorageIds()) {
			
			Configuration storageConfig = config.getStorageConfig(storageId);
			Storage storageInstance;
			try {
				String storageClass = storageConfig.getParameter(Configuration.CLASS_PATH);
				Constructor<?> constructor = Class.forName(storageClass).getConstructor(Configuration.class);
				storageInstance = (Storage) constructor.newInstance(storageConfig);
				
				boolean opened = storageInstance.open();
				if(opened) {
					storages.add(storageInstance);
				}
			} catch (Exception e) {
				throw new Exception("Error during storage initialization", e);
			}
			
			
		}
	}
	
	/**
	 * Stops all consumer threads and all the databases used
	 */
	public void stop() {
		for(Consumer consumer : consumers) {
			consumer.die();
		}
		
		for(Storage storage : storages) {
			storage.close();
		}
		
		state = StorageHandlerState.CLOSE;
	}
	
}