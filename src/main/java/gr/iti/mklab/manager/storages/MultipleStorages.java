package gr.iti.mklab.sfc.storages;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.sfc.streams.StreamsManagerConfiguration;

/**
 * Class for handling store actions for different types of storages
 * (mongoDB, solr, flatfile, redis, lucene ect)
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 */
public class MultipleStorages implements Storage {
	
	private List<Storage> storages = new ArrayList<Storage>();
	private Logger logger = Logger.getLogger(MultipleStorages.class);
	
	public int totalItems = 0;
	public long totalTime = 0l;
	
	public MultipleStorages() {
		
	}
	
	public MultipleStorages(StreamsManagerConfiguration config) {
		for (String storageId : config.getStorageIds()) {
			Configuration storage_config = config.getStorageConfig(storageId);
			Storage storage_instance;
			try {
				String storageClass = storage_config.getParameter(Configuration.CLASS_PATH);
				Constructor<?> constructor
					= Class.forName(storageClass).getConstructor(Configuration.class);
				storage_instance = (Storage) constructor.newInstance(storage_config);
				
			} catch (Exception e) {
				logger.error(e);
				return;
			}
			
			this.register(storage_instance);
		}
	}
	
	@Override
	public boolean open() {
		synchronized(storages) {
			for(Storage storage : storages) {
				try {
					storage.open();
				}
				catch(Exception e) {
					logger.error("Error during opening " + storage.getStorageName(), e);
				}
			}
		}
		return true;
	}
	
	public boolean open(Storage storage) {
		return storage.open();
	}
	
	@Override
	public void store(Item item) throws IOException {
		synchronized(storages) {
			long time = System.currentTimeMillis();
			for(Storage storage : storages) {
				try {
					storage.store(item);
				}
				catch(Exception e) {
					logger.error(e);
					return;
				}
			}
			totalItems++;
			totalTime += (System.currentTimeMillis() - time);
		}
	}
	
	@Override
	public void update(Item item) throws IOException {
		synchronized(storages) {
			for(Storage storage : storages) {
				try {
					storage.update(item);
				}
				catch(Exception e) {
					continue;
				}
			}
		}
	}	
	
	@Override
	public boolean delete(String id) throws IOException {
		synchronized(storages) {
			boolean deleted = true;
			for(Storage storage : storages) {
				try {
					deleted = deleted && storage.delete(id);
				}
				catch(Exception e) {
					deleted = false;
					continue;
				}
			}
			return deleted;
		}
	}

	@Override
	public void close() {
		for(Storage storage : storages) {
			try {
				storage.close();
			}
			catch(Exception e) {
				logger.error(e);
				continue;
			}
		}
		storages.clear();
	}

	public void register(Storage storage) {
		logger.info("Register storage "+storage.getStorageName());
		synchronized(storages) {
			storages.add(storage);
		}
	}
	
	public void remove(Storage storage) {
		logger.info("Remove storage "+storage.getStorageName());
		synchronized(storages) {
			storages.remove(storage);
		}
	}
	
	public List<Storage> getRegisteredStorages() {
		return storages;
	}

	@Override
	public void updateTimeslot() {
		synchronized(storages) {
			for(Storage storage : storages) {
				storage.updateTimeslot();
			}
		}
	}

	@Override
	public boolean checkStatus() {
		boolean status = true;
		for(Storage storage : storages) {
			status = status && storage.checkStatus();
		}
		return status;
	}
	
	@Override
	public String getStorageName() {
		return null;
	}
}
