package gr.iti.mklab.sm.storages;

import java.io.IOException;

import gr.iti.mklab.simmo.core.documents.Post;
import gr.iti.mklab.simmo.core.documents.Webpage;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.items.Video;
import gr.iti.mklab.simmo.core.morphia.DAOManager;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import gr.iti.mklab.sm.Configuration;

import org.apache.log4j.Logger;

import com.mongodb.MongoException;

/**
 * Class for storing items in mongo db
 *
 * @author manosetro
 * @email manosetro@iti.gr
 */
public class MongoDbStorage implements Storage {

    private static String HOST = "mongodb.host";
    private static String DB = "mongodb.database";
    private static String USERNAME = "mongodb.username";
    private static String PASSWORD = "mongodb.password";
    
    private Logger logger = Logger.getLogger(MongoDbStorage.class);

    private String storageName = "Mongodb";

    private String host;
    private String database;

    private String username = null;
    private String password = null;

    private DAOManager dao = null;

    public MongoDbStorage(Configuration config) {
        this.host = config.getParameter(MongoDbStorage.HOST);
        this.database = config.getParameter(MongoDbStorage.DB);
        
        this.username = config.getParameter(MongoDbStorage.USERNAME);
        this.password = config.getParameter(MongoDbStorage.PASSWORD);
        
    }

    @Override
    public void close() {
        MorphiaManager.tearDown();
    }

    @Override
    public boolean delete(String id) throws IOException {

        return false;
    }

    @Override
    public boolean open() {
        logger.info("Open MongoDB storage <host: " + host + ">");
        if (database != null) {
            try {
            	if(username != null && !username.equals("") && password != null && !password.equals("")) {
            		MorphiaManager.setup(host, username, password);
            	}
            	else {
            		MorphiaManager.setup(host);
            	}
                
                dao = new DAOManager(database);
            } catch (Exception e) {
                logger.error("MongoDB Storage failed to open!");
                return false;
            }
        }
        return true;
    }

    @Override
    public void store(gr.iti.mklab.simmo.core.Object object) throws IOException {
        try {

            if (object instanceof Image) {
                dao.userDAO.save(object.getContributor());
                dao.imageDAO.save((Image) object);
            } else if (object instanceof Video){ 
                dao.userDAO.save(object.getContributor());
                dao.videoDAO.save((Video) object);
            }
            else if (object instanceof Webpage) {
            	dao.saveWebpage((Webpage) object);
            }
            else {
                dao.savePost((Post) object);
            }
        } catch (MongoException e) {
            e.printStackTrace();
            logger.error("Storing item " + object.getId() + " failed.");
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
