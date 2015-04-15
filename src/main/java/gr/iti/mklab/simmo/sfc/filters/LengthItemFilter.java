package gr.iti.mklab.simmo.sfc.filters;

import java.util.List;

import gr.iti.mklab.framework.Configuration;
import gr.iti.mklab.simmo.documents.Post;

import org.apache.log4j.Logger;

public class LengthItemFilter extends ItemFilter {

	private int minTextLenth = 15;

	public LengthItemFilter(Configuration configuration) {
		super(configuration);
		String lenStr =configuration.getParameter("length", "15");
		this.minTextLenth  = Integer.parseInt(lenStr);
		
		Logger.getLogger(LengthItemFilter.class).info("Initialized. Min Text Lenth: " + minTextLenth);
	}
	
	@Override
	public boolean accept(Post post) {
		if(post == null) {
			incrementDiscarded();
			return false;
		}
		
		String title = post.getTitle();
		if(title == null) {
			incrementDiscarded();
			return false;
		}
		
		try {
			List<String> tags = post.getTags();
			if(tags != null) {
				for(String tag : tags) {
					title = title.replaceAll(tag, " ");
				}
			}
		
		
			title = title.replaceAll("#", " ");
			title = title.replaceAll("@", " ");
			title = title.replaceAll("\\s+", " ");
		}
		catch(Exception e) {
			
		}
		
		if(title.length() < minTextLenth) {
			incrementDiscarded();
			return false;
		}
		
		incrementAccepted();
		return true;
	}

	@Override
	public String name() {
		return "LengthItemFilter";
	}
}
