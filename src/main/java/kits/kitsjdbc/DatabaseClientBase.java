package kits.kitsjdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import kits.kitsjdbc.NamedParameteredStatementParser.Result;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class DatabaseClientBase {

	private static final Logger logger = LoggerFactory.getLogger(DatabaseClientBase.class);
	
	protected final DataSource dataSource;
	
	public DatabaseClientBase(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	protected int insert(Connection connection, String tableName, DataMap parameters) {
		try {
			Set<String> columnNames = parameters.keys();
			String insertString  = "INSERT INTO " + tableName + "(" + String.join(", ", columnNames) + ") VALUES (" + generateQuestionMarks(parameters.size()) + ")";
			logger.debug("Inserting data with sql: '" + insertString + "' and parameters: " + parameters);
			try(PreparedStatement statement = connection.prepareStatement(insertString)){
				int index = 1;
				for(String columnName : columnNames) {
					statement.setObject(index, transformToJdbcValue(parameters.data(columnName)));
					index++;
				}
				return statement.executeUpdate();
			}
		} catch(SQLException e) {
			throw new RuntimeException("Error executing insert", e);
		}
	}
	
	/*
	 * According to the jdbc 4.2 spec it should not be required, but Derby can not handle java.time values 
	 */
	private Object transformToJdbcValue(Object value) {
		if(value instanceof LocalDate) {
			return Date.from(((LocalDate)value).atStartOfDay(ZoneId.systemDefault()).toInstant());
		} else if(value instanceof LocalDateTime) {
			return Date.from(((LocalDateTime)value).atZone(ZoneId.systemDefault()).toInstant());
		} else {
			return value;
		}
	}
	
	private String generateQuestionMarks(int n) {
		List<String> questionMarks = new LinkedList<>();
		for(int i=0;i<n;i++){
			questionMarks.add("?");
		}
		return String.join(", ", questionMarks);
	}
	
	protected int update(Connection connection, String updateSqlString, DataMap parameters) {
		PreparedStatement statement = null;
		try {
			logger.debug("Updating data with sql: '" + updateSqlString + "' and parameters: " + parameters);
			
			statement = createStatement(connection, updateSqlString, parameters);
			int affectedRows = statement.executeUpdate();
			logger.debug(affectedRows + " rows were affected");
			return affectedRows;
		} catch(SQLException e) {
			throw new RuntimeException("Error executing update", e);
		} finally {
			closeStatement(statement);
		}
	}
	
	protected PreparedStatement createStatement(Connection connection, String sqlString, DataMap parameters) {
		Result result = NamedParameteredStatementParser.parseNamedParameteredStatement(sqlString);
		
		List<String> parameterNames = result.parameterNames;
		
		if(new HashSet<>(parameterNames).size() != parameters.size()){
			throw new IllegalArgumentException("Number of parameters does not match the number of different parameter names");
		}
		
		try {
			PreparedStatement statement = connection.prepareStatement(result.parameteredStatementString);
			for(int index=0;index<parameterNames.size();index++) {
				String parameterName = parameterNames.get(index);
				Object parameter = parameters.data(parameterName);
				if(parameter == null){
					throw new IllegalArgumentException("No parameter provided with name '" + parameterName + " '");
				}
				statement.setObject(index+1, transformToJdbcValue(parameter));
			}
			return statement;
		} catch(SQLException e) {
			throw new RuntimeException("Error executing update", e);
		}
	}
	
	protected void closeStatement(PreparedStatement statement) {
		if(statement != null) {
			try{
				statement.close();
			} catch(SQLException e) {
				throw new RuntimeException("Error closing statment", e);
			}
		}
	}
	
}
