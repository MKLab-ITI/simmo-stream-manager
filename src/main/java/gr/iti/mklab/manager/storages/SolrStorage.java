package gr.iti.mklab.manager.storages;

import gr.iti.mklab.manager.config.Configuration;
import gr.iti.mklab.simmo.UserAccount;
import gr.iti.mklab.simmo.documents.Post;
import gr.iti.mklab.simmo.util.Location;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.beans.Field;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

/**
 * Class for storing items to solr
 * @author manosetro
 * @email  manosetro@iti.gr
 *
 */
public class SolrStorage implements Storage {

	private Logger  logger = Logger.getLogger(SolrStorage.class);
	
	private static final String HOSTNAME = "solr.hostname";
	private static final String SERVICE = "solr.service";
	
	private static final String ITEMS_COLLECTION = "solr.items.collection";
	
	private String hostname, service;
	
	private String collection = null;
	
	private String storageName = "Solr";

	private HttpSolrServer server;
	
	public SolrStorage(Configuration config) throws IOException {
		this.hostname = config.getParameter(SolrStorage.HOSTNAME);
		this.service = config.getParameter(SolrStorage.SERVICE);
		this.collection = config.getParameter(SolrStorage.ITEMS_COLLECTION);
	}
	
	@Override
	public boolean open() {
		try {
			if(collection != null) {
				String baseURL = hostname+"/"+service+"/"+collection;
				server = new HttpSolrServer(baseURL);
			}
		} catch (Exception e) {
			logger.error(e);
			return false;
		}
		return true;	
	}

	@Override
	public void store(Post post) throws IOException {		
		if(server != null) {
			PostBean bean = new PostBean(post);
			try {
				server.addBean(bean);
			} catch (SolrServerException e) {
				logger.error(e);
			}
		}
	}
	

	@Override
	public void update(Post update) throws IOException {
		store(update);
	}
	
	@Override
	public boolean delete(String postId) throws IOException {
		logger.info("Delete post with id " + postId + " from Solr.");
		try {
			server.deleteById(postId);
		} catch (SolrServerException e) {
			logger.error(e);
		}
		return true;
	}
	
	@Override
	public boolean checkStatus() {
		return false;
	}
	

	@Override
	public void close() {

	}
	
	@Override
	public String getStorageName() {
		return this.storageName;
	}

	/**
	 *
	 * @author 	Manos Schinas
	 * @email	manosetro@iti.gr
	 * 
	 */
	public static class PostBean {

		@Field(value = "id")
	    private String id;
	    
	    @Field(value = "title")
	    private String title;
	    
	    @Field(value = "description")
	    private String description;
	    
	    @Field(value = "tags")
	    private List<String> tags;

	    @Field(value = "language")
	    private String language;
	    
	    @Field(value = "publicationTime")
	    private long publicationTime;
	    
	    @Field(value = "contributorId")
	    private String contributorId;
	    
	    @Field(value = "latitude")
	    private Double latitude;
	    
	    @Field(value = "longitude")
	    private Double longitude;
	    
	    @Field(value = "city")
	    private String city;
	    
	    @Field(value = "country")
	    private String country;

	    public PostBean(Post post) {

	        id = post.getId();
	        title = post.getTitle();
	        description = post.getDescription();
	        tags = post.getTags();
	        language = post.getLanguage();
	        publicationTime = post.getCreationDate().getTime();

	        Location location = post.getLocation();
	        if(location != null) {
	        	double[] coordinates = location.getCoordinates();
	        	if(coordinates != null && coordinates.length == 2) {
	        		latitude = coordinates[0];
	        		longitude = coordinates[1];	
	        	}
	        	city = location.getCity();
	        	country = location.getCountry();
	        }
	        
	        UserAccount contributor = post.getContributor();
	        if(contributor != null) {
	        	contributorId = contributor.getId();
	        }
	    }
	}

}
