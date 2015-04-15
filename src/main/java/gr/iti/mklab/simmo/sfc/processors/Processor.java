package gr.iti.mklab.simmo.sfc.processors;

import gr.iti.mklab.framework.Configuration;
import gr.iti.mklab.simmo.documents.Post;

public abstract class Processor {

	protected Configuration configuration;

	public Processor(Configuration configuration) {
		this.configuration = configuration;
	}
	
	public abstract  void process(Post item);
	
}
