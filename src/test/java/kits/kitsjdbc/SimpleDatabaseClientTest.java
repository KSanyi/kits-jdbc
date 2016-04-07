package kits.kitsjdbc;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

public class SimpleDatabaseClientTest extends DatabaseClientTestBase {

	private SimpleDatabaseClient simpleDatabaseClient = new SimpleDatabaseClient(dataSource);
	
	@Test
	public void dateMapping() {
		DataMap dataMap = simpleDatabaseClient.loadDataMap("SELECT * FROM EMPLOYEE WHERE BIRTHDATE = :birthDate", new DataMap("birthDate", LocalDate.of(1994, 2, 23))).get();
		Assert.assertEquals("E0001", dataMap.data("EMPLOYEE_ID"));
	}
	
	@Test
	public void singleStringLoad() {
		String employerName = simpleDatabaseClient.loadString("SELECT NAME FROM EMPLOYER WHERE EMPLOYER_ID = :employerId", new DataMap("employerId", "EMP001")).get();
		Assert.assertEquals("PWC", employerName);
	}
	
	@Test
	public void singleDataLoadWithParameters() {
		Optional<DataMap> dataMap = simpleDatabaseClient.loadDataMap("SELECT * FROM EMPLOYER WHERE EMPLOYER_ID = :employerId", new DataMap("employerId", "EMP001"));
		Assert.assertTrue(dataMap.isPresent());
		Assert.assertEquals("PWC", dataMap.get().data("NAME"));
	}
	
	@Test
	public void multipleDataLoad() {
		List<DataMap> dataMaps = simpleDatabaseClient.loadDataMapList("SELECT * FROM EMPLOYER ORDER BY EMPLOYER_ID");
		Assert.assertEquals(2, dataMaps.size());
		Assert.assertEquals(new DataMap("EMPLOYER_ID", "EMP001", "NAME", "PWC"), dataMaps.get(0));
		Assert.assertEquals(new DataMap("EMPLOYER_ID", "EMP002", "NAME", "EY"), dataMaps.get(1));
	}
	
	@Test
	public void multipleDataLoadWithParameters() {
		List<DataMap> dataMaps = simpleDatabaseClient.loadDataMapList("SELECT E1.NAME AS E1NAME, E2.NAME AS E2NAME FROM EMPLOYER E1, EMPLOYER E2 WHERE E1.EMPLOYER_ID = :employerId OR E2.EMPLOYER_ID = :employerId", new DataMap("employerId", "EMP001"));
		Assert.assertEquals(3, dataMaps.size());
	}
	
}
