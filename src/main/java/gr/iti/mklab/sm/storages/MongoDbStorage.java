package gr.iti.mklab.sm.storages;

import java.io.IOException;

import gr.iti.mklab.simmo.core.documents.Post;
import gr.iti.mklab.simmo.core.morphia.DAOManager;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import gr.iti.mklab.sm.Configuration;

import org.apache.log4j.Logger;

import com.mongodb.MongoException;

/**
 * Class for storing items in mongo db
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 *
 */
public class MongoDbStorage implements Storage {

	private static String HOST = "mongodb.host";
	private static String DB = "mongodb.database";
	
	private Logger logger = Logger.getLogger(MongoDbStorage.class);
	
	private String storageName = "Mongodb";
	
	private String host;
	private String database;
	
	private DAOManager dao = null;
	
	public MongoDbStorage(Configuration config) {
		this.host = config.getParameter(MongoDbStorage.HOST);
		this.database = config.getParameter(MongoDbStorage.DB);
	}
	
	@Override
	public void close() {
		
	}

	@Override
	public boolean delete(String id) throws IOException {
		
		return false;
	}
	
	@Override
	public boolean open() {
		logger.info("Open MongoDB storage <host: " + host + ">");
		if(database != null) {
			try {
				MorphiaManager.setup(host);
				
				dao = new DAOManager(database);	
			} catch (Exception e) {
				logger.error("MongoDB Storage failed to open!");
				return false;
			}
		}
		return true;
	}

	
	@Override
	public void store(Post post) {
		try {
			dao.savePost(post);	
		}
		catch(MongoException e) {
			e.printStackTrace();
			logger.error("Storing item " + post.getId() + " failed.");
		}
	
	}

	
	@Override
	public boolean checkStatus() {
		return true;
	}
	
	@Override
	public String getStorageName() {
		return this.storageName;
	}
	
	/*
	private class UpdaterTask extends Thread {

		private long timeout = 10 * 60 * 1000;
		private boolean stop = true;
		
		@Override
		public void run() {
			stop = false;
			while(!stop) {
				try {
					
					synchronized(this) {
						this.wait(timeout);
					}
					
					logger.info("Update: ");
					long t = System.currentTimeMillis();
					
					synchronized(itemsMap) {
						logger.info(itemsMap.size() + " items to update");
						for(Item item : itemsMap.values()) {
							//itemDAO.updateItem(item);
						}
						itemsMap.clear();
					}
					
					synchronized(usersMap) {
						logger.info(usersMap.size() + " users to update");
						for(Entry<String, StreamUser> user : usersMap.entrySet()) {
							//streamUserDAO.updateStreamUserStatistics(user.getValue());
						}
						usersMap.clear();
					}
					
					if(webPageDAO != null) {
						synchronized(webpagesSharesMap) {
							logger.info(webpagesSharesMap.size() + " web pages to update");
							for(Entry<String, Integer> e : webpagesSharesMap.entrySet()) {
								//webPageDAO.updateWebPageShares(e.getKey(), e.getValue());
							}
							webpagesSharesMap.clear();
						}
					}
					
					if(mediaItemDAO != null) {
						synchronized(mediaItemsSharesMap) {
							logger.info(mediaItemsSharesMap.size() + " media Items to update");
							for(Entry<String, Integer> entry : mediaItemsSharesMap.entrySet()) {
								//mediaItemDAO.updateMediaItemShares(entry.getKey(), entry.getValue());
							}
							mediaItemsSharesMap.clear();
						}
					}
					
					t = System.currentTimeMillis() - t;
					logger.info("Mongo Updates took " + t + " milliseconds");
					logger.info("======================================");
					
				} catch (Exception e) {
					if(stop) {
						logger.info("Mongo updater thread interrupted from sleep to stop");
					}
					else {
						logger.error("Exception in mongo updater thread. ", e);
						logger.info(mediaItemsSharesMap.size() + " media Items to update");
						logger.info(webpagesSharesMap.size() + " web pages to update");
						logger.info(usersMap.size() + " users to update");
						logger.info(itemsMap.size() + " items to update");
						
						mediaItemsSharesMap.clear();
						webpagesSharesMap.clear();
						usersMap.clear();
						itemsMap.clear();
						
					}
					continue;
				}
			}
		}
		
		public void stopTask() {
			logger.info("Stop updater task");
			try {
				this.stop = true;
				this.interrupt();
			}
			catch(Exception e) {
				logger.error("Fail to stop update task in MongoDBStorage", e);
			}
		}
		
	}
	*/
	
}
