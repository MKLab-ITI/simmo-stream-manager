package gr.iti.mklab.manager.storages;

import gr.iti.mklab.manager.config.Configuration;
import gr.iti.mklab.simmo.documents.Post;

import java.io.IOException;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
/**
 * Class for storing items to redis store
 * @author manosetro
 * @email  manosetro@iti.gr
 *
 */
public class RedisStorage implements Storage {

	private static String HOST = "redis.host";
	private static String WEBPAGES_CHANNEL = "redis.webpages.channel";
	private static String MEDIA_CHANNEL = "redis.media.channel";
	private static String ITEMS_CHANNEL = "redis.items.channel";
	
	private Logger  logger = Logger.getLogger(RedisStorage.class);
	
	private JedisPool jedisPool;
	private Jedis jedis;
	private String host;
	
	private String channel = null;
	
	private long items = 0, mItems = 0, wPages = 0;
	
	private String storageName = "Redis";
	
	
	public RedisStorage(Configuration config) {
		this.host = config.getParameter(RedisStorage.HOST);
		this.channel = config.getParameter(RedisStorage.ITEMS_CHANNEL);
	}
	
	@Override
	public boolean open() {
		try {
			JedisPoolConfig poolConfig = new JedisPoolConfig();
			jedisPool = new JedisPool(poolConfig, host, 6379, 0);
		
			this.jedis = jedisPool.getResource();
			return true;
		}
		catch(Exception e) {
			logger.error("Error during opening.", e);
			return false;
		}

	}

	@Override
	public void store(Post post) throws IOException {

		if(post != null && channel != null) {
			items++;
			synchronized(jedis) {
				jedis.publish(channel, post.toString());
			}
		}
	}
	
	@Override
	public void update(Post post) throws IOException {
		// Not supported.
	}
	
	@Override
	public boolean delete(String id) throws IOException {
		// Not supported.
		return false;
	}

	@Override
	public void close() {
		jedis.disconnect();
		jedisPool.close();
	}
	
	@Override
	public boolean checkStatus() {
		try {
			logger.info("Redis sent " + items + " items, " + mItems + " media items and " 
					+ wPages + " web pages!");

			boolean connected;
			synchronized(jedis) {
				jedis.info();
				connected = jedis.isConnected();
			}
			if(!connected) {
				connected = reconnect();
			}
			return connected;
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error(e);
			return reconnect();
		}
	}
	
	private boolean reconnect() {
		synchronized(jedis) {
			try {
				if(jedis != null) {
					jedis.disconnect();
				}
			}
			catch(Exception e) { 
				logger.error(e);
			}
		}
		try {
			synchronized(jedis) {
				JedisPoolConfig poolConfig = new JedisPoolConfig();
				jedisPool = new JedisPool(poolConfig, host, 6379, 0);
        	
        		this.jedis = jedisPool.getResource();
        		jedis.info();
        		return jedis.isConnected();
			}
		}
		catch(Exception e) {
			logger.error(e);
			return false;
		}

	}
	
	@Override
	public String getStorageName(){
		return this.storageName;
	}

	public static void main(String...args) {
		System.out.println("Check Redis Storage");
		
		Configuration config = new Configuration();
		config.setParameter(RedisStorage.HOST, "160.40.51.18");
		config.setParameter(RedisStorage.ITEMS_CHANNEL, "items");
		config.setParameter(RedisStorage.WEBPAGES_CHANNEL, "webpages");
		config.setParameter(RedisStorage.MEDIA_CHANNEL, "media");
		RedisStorage redis = new RedisStorage(config);
		redis.open();
		
		redis.checkStatus();
		
	}
}
