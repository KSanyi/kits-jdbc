package kits.kitsjdbc;

import org.apache.derby.jdbc.EmbeddedDataSource;
import org.junit.Before;

public class DatabaseClientTestBase {

	protected final EmbeddedDataSource dataSource;
	
	public DatabaseClientTestBase() {
		dataSource = new EmbeddedDataSource();
		dataSource.setDatabaseName("memory:TESTDB");
		dataSource.setCreateDatabase("create");
	}	

	@Before
	public void init() throws Exception {
		SimpleDatabaseClient databaseClient = new SimpleDatabaseClient(dataSource);
		
		try {
			databaseClient.executeSql("DROP TABLE EMPLOYER");
			databaseClient.executeSql("DROP TABLE EMPLOYEE");	
		} catch (Exception ex) {
			// TABLE does not exist, no problem
		}
		
		databaseClient.executeSql("CREATE TABLE EMPLOYER(EMPLOYER_ID VARCHAR(20), NAME VARCHAR(50))");
		databaseClient.executeSql("CREATE TABLE EMPLOYEE(EMPLOYEE_ID VARCHAR(20), NAME VARCHAR(50), BIRTHDATE DATE, EMPLOYER_ID VARCHAR(20), SALARY INT)");
		
		databaseClient.executeSql("INSERT INTO EMPLOYER VALUES('EMP001', 'PWC')");
		databaseClient.executeSql("INSERT INTO EMPLOYER VALUES('EMP002', 'EY')");
		databaseClient.executeSql("INSERT INTO EMPLOYEE VALUES('E0001', 'John Smith', DATE('1994-02-23'), 'EMP001', 100000)");
	}
	
}
