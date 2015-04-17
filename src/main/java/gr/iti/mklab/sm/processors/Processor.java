package gr.iti.mklab.sm.processors;

import gr.iti.mklab.simmo.core.documents.Post;
import gr.iti.mklab.sm.Configuration;

public abstract class Processor {

	protected Configuration configuration;

	public Processor(Configuration configuration) {
		this.configuration = configuration;
	}
	
	public abstract  void process(Post item);
	
}
