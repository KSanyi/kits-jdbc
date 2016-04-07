package kits.kitsjdbc;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DataMap {

	private final Map<String, Object> map;
	
	public DataMap(Map<String, Object> map) {
		this.map = Collections.unmodifiableMap(map);
	}
	
	public DataMap() {
		map = new HashMap<>();
	}
	
	public DataMap(String key, Object data) {
		this();
		map.put(key, data);
	}
	
	public DataMap(String key1, Object data1, String key2, Object data2) {
		this(key1, data1);
		map.put(key2, data2);
	}
	
	public DataMap(String key1, Object data1, String key2, Object data2, String key3, Object data3) {
		this(key1, data1, key2, data2);
		map.put(key3, data3);
	}
	
	public DataMap(String key1, Object data1, String key2, Object data2, String key3, Object data3, String key4, Object data4) {
		this(key1, data1, key2, data2, key3, data3);
		map.put(key4, data4);
	}
	
	public DataMap(String key1, Object data1, String key2, Object data2, String key3, Object data3, String key4, Object data4, String key5, Object data5) {
		this(key1, data1, key2, data2, key3, data3, key4, data4);
		map.put(key5, data5);
	}
	
	public Object data(String key) {
		return map.get(key);
	}
	
	public Set<String> keys() {
		return map.keySet();
	}
	
	public int size() {
		return map.size();
	}
	
	public Object onlyData() {
		if(map.size() != 1) {
			throw new IllegalArgumentException("Datamap should have only 1 entry");
		}
		return map.values().iterator().next();
	}
	
	@Override
	public String toString() {
		return "DataMap(" + String.join(", ", MapperHelper.mapToList(map.entrySet(), e -> e.getKey() + "->" + e.getValue())) + ")";
	}
	
	@Override
	public boolean equals(Object other) {
		return other instanceof DataMap ? map.equals(((DataMap)other).map) : false;
	}
	
	@Override
	public int hashCode() {
		return map.hashCode();
	}
	
}
