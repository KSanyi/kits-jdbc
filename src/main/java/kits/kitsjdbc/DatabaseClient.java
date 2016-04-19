package kits.kitsjdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

public class DatabaseClient extends DatabaseClientBase {

	public DatabaseClient(DataSource dataSource) {
		super(dataSource);
	}

	public <R> Optional<R> loadData(String query, ResultSetMapper<? extends R> resultSetMapper, DataMap parameters) {
		List<R> dataMaps = loadDataList(query, resultSetMapper, parameters);
		
		if(dataMaps.isEmpty()) {
			return Optional.empty();
		} else if(dataMaps.size() > 1) {
			throw new IllegalArgumentException("Database returned " + dataMaps.size() + " entries");
		} else {
			return Optional.of(dataMaps.get(0));
		}
	}
	
	public <R> List<R> loadDataList(String query, ResultSetMapper<? extends R> resultSetMapper, DataMap parameters) {
		PreparedStatement statement = null;
		try {
			List<R> dataMapList = new LinkedList<>();
			try(Connection connection = dataSource.getConnection()) {
				statement = createStatement(connection, query, parameters);
				
				try(ResultSet rs = statement.executeQuery()){
					while(rs.next()) {
						dataMapList.add(resultSetMapper.apply(rs));
					}
				}
			}
			return Collections.unmodifiableList(dataMapList);
		} catch(SQLException e) {
			throw new RuntimeException("Error during loading data from the database", e);
		} finally {
			closeStatement(statement);
		}
	}
	
	public <R> Optional<R> loadData(String query, ResultSetMapper<? extends R> resultSetMapper) {
		return loadData(query, resultSetMapper, new DataMap());
	}
	
	public <R> List<R> loadDataList(String query, ResultSetMapper<? extends R> resultSetMapper) {
		return loadDataList(query, resultSetMapper, new DataMap());
	}
	
	public void executeSql(String sqlString) {
		update(sqlString);
	}
	
	public void insert(String tableName, DataMap values) {
		try {
			try(Connection connection = dataSource.getConnection()) {
				insert(connection, tableName, values);
			}
		} catch(SQLException ex) {
			throw new RuntimeException("Error executing insert", ex);
		}
	}
	
	public void update(String updateSqlString, DataMap values) {
		try {
			try(Connection connection = dataSource.getConnection()) {
				update(connection, updateSqlString, values);
			}
		} catch(SQLException ex) {
			throw new RuntimeException("Error executing update", ex);
		}
	}
	
	public void updateEntry(String tableName, DataMap values, String idColumnName, Object id)  {
		try {
			try(Connection connection = dataSource.getConnection()) {
				updateEntry(connection, tableName, values, idColumnName, id);
			}
		} catch(SQLException ex) {
			throw new RuntimeException("Error executing update", ex);
		}
	} 
	
	public void update(String updateString) {
		update(updateString, new DataMap());
	}
	
}
