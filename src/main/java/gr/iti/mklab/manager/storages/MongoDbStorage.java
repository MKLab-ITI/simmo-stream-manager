package gr.iti.mklab.manager.storages;

import gr.iti.mklab.manager.config.Configuration;
import gr.iti.mklab.simmo.documents.Post;
import gr.iti.mklab.simmo.morphia.DAOManager;
import gr.iti.mklab.simmo.morphia.MorphiaManager;

import java.io.IOException;
import java.util.Date;

import org.apache.log4j.Logger;

import com.mongodb.WriteResult;

/**
 * Class for storing items in mongo db
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 * 
 * @author ailiakop
 * @email  ailiakop@iti.gr
 *
 */
public class MongoDbStorage implements Storage {

	private static String HOST = "mongodb.host";
	private static String DB = "mongodb.database";

	private Logger logger = Logger.getLogger(MongoDbStorage.class);
	
	private String storageName = "Mongodb";
	
	private String host;
	private String database;

	private Integer items = 0;
	

	private DAOManager dao;

	public MongoDbStorage(Configuration config) {	
		this.host = config.getParameter(MongoDbStorage.HOST);
		this.database = config.getParameter(MongoDbStorage.DB);
	}
	
	@Override
	public void close() {
		 MorphiaManager.tearDown();
	}

	@Override
	public boolean delete(String id) throws IOException {
		WriteResult result = dao.postDAO.deleteById(id);
		if(result.getN() > 0)
			return true;
		
		return false;
	}
	
	@Override
	public boolean open() {
		
		logger.info("Open MongoDB storage <host: " + host + ">");

		MorphiaManager.setup(host);
		dao = new DAOManager(database);

		
		return true;
	}

	
	@Override
	public void store(Post post) {
		try {
			// Handle Items
			items++;
			post.setCrawlDate(new Date());
			dao.savePost(post);
		}
		catch(Exception e) {
			logger.error("Storing post " + post.getId() + " failed.");
		}
	
	}

	@Override
	public void update(Post post) throws IOException {
		// update item
		store(post);
	}

	
	@Override
	public boolean checkStatus() {
		return true;
	}
	
	@Override
	public String getStorageName() {
		return this.storageName;
	}
	
}
