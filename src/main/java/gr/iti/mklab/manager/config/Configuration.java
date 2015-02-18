package gr.iti.mklab.manager.config;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.mongodb.morphia.annotations.Entity;


/**
 * Class for the configuration of streams or storages reading a set of parameters
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 *
 */

@Entity(noClassnameStored = true)
public class Configuration implements Iterable<String> {
	
	public static final String CLASS_PATH = "Classpath";
	
	private Map<String,String> params = new HashMap<String,String>();

	public String getParameter(String name) {
		return getParameter(name,null);
	}
	
	public String getParameter(String name, String defaultValue){
		String value = params.get(name);
		return value == null ? defaultValue : value;
	}
	
	public void setParameter(String name, String value) {
		params.put(name,value);
	}

	@Override
	public Iterator<String> iterator() {
		return params.keySet().iterator();
	}
	
}
