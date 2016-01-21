package gr.iti.mklab.sm.feeds;

import java.util.Date;

import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

@Entity(value="feeds", noClassnameStored=false)
public class Feed {

	@Id
	protected String id = null;

	protected Date since = null;
	
	protected String source;
	
	protected String label;
	
	public Feed() {
		
	}
	
	public Feed(Date since) {
		this.since = since;
	}
	
	public  String getId() {
		return id;
	}
	
	public  void setId(String id) {
		this.id = id;
	}
	
	public  String getLabel() {
		return label;
	}
	
	public  void setLabel(String label) {
		this.label = label;
	}
	
	public  String getSource() {
		return source;
	}
	
	public  void setSource(String source) {
		this.source = source;
	}
	
	public Date getSinceDate() {
		return since;
	}
	
	public void setSinceDate(Date since) {
		this.since = since;
	}
	
	@Entity(noClassnameStored = true)
	public enum FeedType {
		KEYWORDS, ACCOUNT, URL, GROUP
	}
	
	@Override
	public int hashCode() {
		return id.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {

		if(this == obj) {
			return true;
		}
		
        return ((Feed) obj).id.equals(id);
    }
}
