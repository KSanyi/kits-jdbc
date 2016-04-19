package kits.kitsjdbc;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class TransactionalDatabaseClientTest extends DatabaseClientTestBase {

	@Test
	public void dataInsertComitted() {
		TransactionalDatabaseClient databaseClient = new TransactionalDatabaseClient(dataSource);
		databaseClient.startTransaction();
		databaseClient.insert("EMPLOYER", new DataMap("EMPLOYER_ID", "EMPR003", "NAME", "BDO Hungary"));
		databaseClient.commitTransaction();
		
		List<DataMap> dataMaps = new SimpleDatabaseClient(dataSource).loadDataMapList("SELECT * FROM EMPLOYER WHERE EMPLOYER_ID = 'EMPR003'");
		Assert.assertEquals(1, dataMaps.size());
		Assert.assertEquals("BDO Hungary", dataMaps.get(0).data("NAME"));
	}
	
	@Test
	public void dataInsertRolledBack() {
		TransactionalDatabaseClient databaseClient = new TransactionalDatabaseClient(dataSource);
		databaseClient.startTransaction();
		databaseClient.insert("EMPLOYER", new DataMap("EMPLOYER_ID", "EMPR003", "NAME", "BDO Hungary"));
		databaseClient.rollbackTransaction();
		
		List<DataMap> dataMaps = new SimpleDatabaseClient(dataSource).loadDataMapList("SELECT * FROM EMPLOYER WHERE EMPLOYER_ID = 'EMPR003'");
		Assert.assertTrue(dataMaps.isEmpty());
	}
	
	@Test
	public void dataInsertAndUpdateComitted() {
		TransactionalDatabaseClient databaseClient = new TransactionalDatabaseClient(dataSource);
		databaseClient.startTransaction();
		databaseClient.insert("EMPLOYER", new DataMap("EMPLOYER_ID", "EMPR003", "NAME", "BDO Hungary"));
		databaseClient.update("UPDATE EMPLOYER SET NAME = :name WHERE EMPLOYER_ID = :employerId", new DataMap("name", "BDO", "employerId", "EMP001"));
		databaseClient.commitTransaction();
		
		List<DataMap> dataMaps = new SimpleDatabaseClient(dataSource).loadDataMapList("SELECT * FROM EMPLOYER WHERE NAME LIKE 'BDO%'");
		Assert.assertEquals(2, dataMaps.size());
	}
	
}
