package gr.iti.mklab.simmo.sfc.filters;

import java.util.List;

import gr.iti.mklab.framework.Configuration;
import gr.iti.mklab.simmo.associations.Reference;
import gr.iti.mklab.simmo.documents.Post;

import org.apache.log4j.Logger;

/**
 * 
 * @author Manos Schinas - manosetro@iti.gr
 *
 * This filter discard items that have many embedded URLs as possible spam.
 * 	
 */
public class UrlItemFilter extends ItemFilter {

	private int maxUrls = 4;

	public UrlItemFilter(Configuration configuration) {
		super(configuration);
		String lenStr =configuration.getParameter("maxUrls", "4");
		this.maxUrls  = Integer.parseInt(lenStr);
		
		Logger.getLogger(UrlItemFilter.class).info("Initialized. Max Number of URLs: " + maxUrls);
	}
	
	@Override
	public boolean accept(Post post) {
		if(post == null) {
			incrementDiscarded();
			return false;
		}
		
		List<Reference> links = post.getLinks();
		if(links == null) {
			incrementAccepted();
			return true;
		}
		
		if(links.size() >= maxUrls) {
			incrementDiscarded();
			return false;
		}
		
		incrementAccepted();
		return true;
	}

	@Override
	public String name() {
		return "UrlItemFilter";
	}
	
}
