package gr.iti.mklab.sm;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.mongodb.morphia.annotations.Entity;

/**
 * Class for the configuration of streams or storages 
 * reading a set of parameters
 * 
 * @author manosetro
 *
 */

@Entity(noClassnameStored = true)
public class Configuration implements Iterable<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5070483137103099259L;
	
	public static final String CLASS_PATH = "Classpath";
	
	private Map<String, String> params = new HashMap<String, String>();

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
