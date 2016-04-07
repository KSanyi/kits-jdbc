package kits.kitsjdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.sql.DataSource;

/**
 * Do not use to load any type of date values
 */
public class SimpleDatabaseClient {

	private final DatabaseClient databaseClient;
	
	private ResultSetMapper<DataMap> resultSetMapper = (ResultSet rs) -> {
		try{
			ResultSetMetaData metaData = rs.getMetaData();
			int columnCount = metaData.getColumnCount();
			Map<String, Object> map = new HashMap<>();
			for(int i=0;i<columnCount;i++) {
				String columnName = metaData.getColumnLabel(i+1);
				Object value = rs.getObject(columnName);
				map.put(columnName, value);
			}
			return new DataMap(map);	
		} catch (SQLException e) {
			throw new RuntimeException("Error during loading data from the database", e);
		}
	};
	
	public SimpleDatabaseClient(DataSource dataSource) {
		databaseClient = new DatabaseClient(dataSource);
	}

	public Optional<DataMap> loadDataMap(String query, DataMap parameters) {
		return databaseClient.loadData(query, resultSetMapper, parameters);
	}
	
	public Optional<DataMap> loadDataMap(String query) {
		return loadDataMap(query, new DataMap());
	}
	
	public List<DataMap> loadDataMapList(String query, DataMap parameters) {
		return databaseClient.loadDataList(query, resultSetMapper, parameters);
	}
	
	public List<DataMap> loadDataMapList(String query) {
		return loadDataMapList(query, new DataMap());
	}
	
	public void executeSql(String sqlString) {
		databaseClient.executeSql(sqlString);
	}
	
	public Optional<String> loadString(String query, DataMap parameters) {
		return loadDataMap(query, parameters).map(dataMap -> dataMap.onlyData().toString());
	}
	
	public Optional<String> loadString(String query) {
		return loadDataMap(query).map(dataMap -> dataMap.onlyData().toString());
	}
	
}
