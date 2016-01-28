package gr.iti.mklab.sm.storages;

import com.mongodb.MongoException;
import gr.iti.mklab.simmo.core.documents.Post;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.items.Video;
import gr.iti.mklab.simmo.core.morphia.DAOManager;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import gr.iti.mklab.sm.Configuration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Class for storing items in mongo db. The difference from simple
 * MongoDbStorage is that this class stores every Object in the collection
 * which is specified in its label field
 *
 * @author kandreadou
 */
public class MongoDBSplitStorage implements Storage {

    private static String HOST = "mongodb.host";

    private Logger logger = Logger.getLogger(MongoDBSplitStorage.class);

    private String storageName = "Mongodb";

    private String host;

    public MongoDBSplitStorage(Configuration config) {
        this.host = config.getParameter(HOST);
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
        try {
            MorphiaManager.setup(host);

        } catch (Exception e) {
            logger.error("MongoDB Storage failed to open!");
            return false;
        }

        return true;
    }

    @Override
    public void store(gr.iti.mklab.simmo.core.Object object) throws IOException {
        try {
            DAOManager dao = DAOManagerFactory.getDAOManager(object.getLabel());
            if (object instanceof Image) {
                if (object.getContributor() != null) {
                    dao.userDAO.save(object.getContributor());
                    dao.imageDAO.save((Image) object);
                }
            } 
            else if (object instanceof Video) {
                if (object.getContributor() != null) {
                    dao.userDAO.save(object.getContributor());
                    dao.videoDAO.save((Video) object);
                }
            } 
            else {
                dao.savePost((Post) object);
            }
            
        } catch (MongoException e) {
            e.printStackTrace();
            logger.error("Storing item " + object.getId() + " failed.");
        } catch (ExecutionException e) {
            e.printStackTrace();
            logger.error("Storing item " + object.getId() + " failed because gettind DAOManager from cache failed");
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

}
