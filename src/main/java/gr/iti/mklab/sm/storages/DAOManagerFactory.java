package gr.iti.mklab.sm.storages;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import gr.iti.mklab.simmo.core.morphia.DAOManager;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A factory for DAOManagers, which holds a cache of instances that
 * expire after one hour of not being accessed.
 *
 * @author kandreadou
 */
public class DAOManagerFactory {

    private static LoadingCache<String, DAOManager> MANAGERS_CACHE = CacheBuilder.newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build(
                    new CacheLoader<String, DAOManager>() {
                        public DAOManager load(String collection) throws Exception {
                            return new DAOManager(collection);
                        }
                    });


    public static DAOManager getDAOManager(String collection) throws ExecutionException {
        return MANAGERS_CACHE.get(collection);
    }

    public static void main(String[] args) throws Exception{
        MorphiaManager.setup("127.0.0.1");
        DAOManagerFactory.getDAOManager("t1");
        DAOManagerFactory.getDAOManager("t2");
        DAOManagerFactory.getDAOManager("t3");
        DAOManagerFactory.getDAOManager("t4");
        DAOManagerFactory.getDAOManager("t5");
        DAOManagerFactory.getDAOManager("t6");
        DAOManagerFactory.getDAOManager("t7");
        MorphiaManager.tearDown();
    }
}
