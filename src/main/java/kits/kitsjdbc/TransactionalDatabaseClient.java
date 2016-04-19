package kits.kitsjdbc;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalDatabaseClient extends DatabaseClientBase {

	private static final Logger logger = LoggerFactory.getLogger(TransactionalDatabaseClient.class);
	
	private Connection connection;
	
	public TransactionalDatabaseClient(DataSource dataSource) {
		super(dataSource);
	}

	public void startTransaction() {
		if(connection != null) {
			throw new IllegalStateException("Transaction is already started");
		}
		try {
			connection = dataSource.getConnection();
			connection.setAutoCommit(false);
			logger.debug("Transaction started");
		} catch (SQLException ex) {
			throw new RuntimeException("Error during loading data from the database", ex);
		}
	}
	
	private void checkTransactionStarted() {
		if(connection == null) {
			throw new IllegalStateException("Transaction is not started");
		}
	}
	
	public void commitTransaction() {
		checkTransactionStarted();
		try {
			connection.commit();
			connection = null;
			logger.debug("Transaction is comitted");
		} catch (SQLException ex) {
			throw new RuntimeException("Error comitting transaction", ex);
		}
	}
	
	public void rollbackTransaction() {
		checkTransactionStarted();
		try {
			connection.rollback();
			connection = null;
			logger.debug("Transaction is rolled back");
		} catch (SQLException ex) {
			throw new RuntimeException("Error rolling back transaction", ex);
		}
	}
	
	public int insert(String tableName, DataMap parameters) {
		checkTransactionStarted();
		try {
			return insert(connection, tableName, parameters);	
		} catch(Throwable t) {
			if(connection != null) {
				try {
					connection.rollback();
					logger.debug("Transaction is rolled back");
				} catch(SQLException ex) {
					logger.error("Error rolling back transaction", ex);
				}
			}
			throw t;
		}
		
	}
	
	public int update(String updateSql, DataMap parameters) {
		checkTransactionStarted();
		try {
			return update(connection, updateSql, parameters);	
		} catch(Throwable t) {
			if(connection != null) {
				try {
					connection.rollback();
					logger.debug("Transaction is rolled back");
				} catch(SQLException ex) {
					logger.error("Error rolling back transaction", ex);
				}
			}
			throw t;
		}
	}
	
	public void updateEntry(String tableName, DataMap values, String idColumnName, Object id)  {
		checkTransactionStarted();
		try {
			updateEntry(connection, tableName, values, idColumnName, id);	
		} catch(Throwable t) {
			if(connection != null) {
				try {
					connection.rollback();
					logger.debug("Transaction is rolled back");
				} catch(SQLException ex) {
					logger.error("Error rolling back transaction", ex);
				}
			}
			throw t;
		}
	} 
	
}
