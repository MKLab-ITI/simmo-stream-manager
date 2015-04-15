package gr.iti.mklab.simmo.sfc.filters;

import gr.iti.mklab.framework.Configuration;
import gr.iti.mklab.simmo.documents.Post;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;


public class LanguageItemFilter extends ItemFilter {

	private Set<String> languages = new HashSet<String>();

	public LanguageItemFilter(Configuration configuration) {
		super(configuration);
		String langsStr = configuration.getParameter("lang", "en");
		
		String[] langs = langsStr.split(",");
		for(String lang : langs) {
			if(lang != null) {
				languages.add(lang.trim());
			}
		}
		Logger.getLogger(LanguageItemFilter.class).info("Supported languages: " + langsStr);
	}
	
	@Override
	public boolean accept(Post post) {
		
		String lang = post.getLanguage();
		if(lang == null) {
			incrementDiscarded();
			return false;
		}
		
		if(!languages.contains(lang)) {
			incrementDiscarded();
			return false;
		}
		
		incrementAccepted();
		return true;
	}

	@Override
	public String name() {
		return "LanguageItemFilter";
	}

}
