package kits.kitsjdbc;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class DatabaseClientTest extends DatabaseClientTestBase {

	private SimpleDatabaseClient simpleDbClient = new SimpleDatabaseClient(dataSource);
	
	private class Employer{
		final String id;
		final String name;
		Employer(String id, String name){
			this.id = id;
			this.name = name;
		}
	}
	
	@Test
	public void singleDataLoadWithParameters() {
		DatabaseClient databaseClient = new DatabaseClient(dataSource);
		ResultSetMapper<Employer> resultsetMapper = rs -> new Employer(rs.getString("EMPLOYER_ID"), rs.getString("NAME"));
		Employer employer = databaseClient.loadData("SELECT * FROM EMPLOYER WHERE EMPLOYER_ID = :employerId", resultsetMapper, new DataMap("employerId", "EMP001")).get();
		Assert.assertEquals("EMP001", employer.id);
		Assert.assertEquals("PWC", employer.name);
	}
	
	@Test
	public void dataInsert() {
		new DatabaseClient(dataSource).insert("EMPLOYER", new DataMap("EMPLOYER_ID", "EMPR003", "NAME", "BDO Hungary"));
		
		List<DataMap> dataMaps = simpleDbClient.loadDataMapList("SELECT * FROM EMPLOYER WHERE EMPLOYER_ID = 'EMPR003'");
		Assert.assertEquals(1, dataMaps.size());
		Assert.assertEquals("BDO Hungary", dataMaps.get(0).data("NAME"));
	}
	
	@Test
	public void dataUpdate() {
		new DatabaseClient(dataSource).update("UPDATE EMPLOYER SET NAME = :name WHERE EMPLOYER_ID = :employerId", new DataMap("name", "PWC Group", "employerId", "EMP001"));
		
		DataMap result = simpleDbClient.loadDataMap("SELECT * FROM EMPLOYER WHERE EMPLOYER_ID = 'EMP001'").get();
		Assert.assertEquals("PWC Group", result.data("NAME"));
	}
	
	@Test
	public void datadelete() {
		Assert.assertTrue(simpleDbClient.loadDataMap("SELECT * FROM EMPLOYER WHERE EMPLOYER_ID = 'EMP001'").isPresent());
		
		new DatabaseClient(dataSource).update("DELETE FROM EMPLOYER WHERE EMPLOYER_ID = :employerId", new DataMap("employerId", "EMP001"));
		
		Assert.assertFalse(simpleDbClient.loadDataMap("SELECT * FROM EMPLOYER WHERE EMPLOYER_ID = 'EMP001'").isPresent());
	}
	
}
