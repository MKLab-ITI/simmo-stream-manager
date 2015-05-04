package gr.iti.mklab.sm.filters;

import java.util.Set;

import gr.iti.mklab.simmo.core.documents.Post;
import gr.iti.mklab.sm.Configuration;

import org.apache.log4j.Logger;

/**
 * 
 * @author Manos Schinas - manosetro@iti.gr
 *
 * This filter discard items that have many hashtags as possible spam.
 * 	
 */
public class TagsItemFilter extends ItemFilter {

	private int maxTags = 4;
	
	public TagsItemFilter(Configuration configuration) {
		super(configuration);
		String lenStr =configuration.getParameter("maxTags", "4");
		this.maxTags  = Integer.parseInt(lenStr);
		
		Logger.getLogger(TagsItemFilter.class).info("Initialized. Max Number of Tags: " + maxTags);
	}
	
	@Override
	public boolean accept(Post post) {
		if(post == null) {
			incrementDiscarded();
			return false;
		}
		
		Set<String> tags = post.getTags();
		if(tags == null) {
			incrementAccepted();
			return true;
		}
		
		if(tags.size() >= maxTags) {
			incrementDiscarded();
			return false;
		}
		
		incrementAccepted();
		return true;
	}

	@Override
	public String name() {
		return "TagsItemFilter";
	}
	
}
