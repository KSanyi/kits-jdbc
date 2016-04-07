package kits.kitsjdbc;

import kits.kitsjdbc.NamedParameteredStatementParser.Result;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class NamedParameteredStatementParserTest {

	@Test
	public void withoutParameters() {
		String namedParameterStatementString = "SELECT * FROM CUSTOMER";
		Result result = NamedParameteredStatementParser.parseNamedParameteredStatement(namedParameterStatementString);
		Assert.assertEquals(namedParameterStatementString, result.parameteredStatementString);
		Assert.assertTrue(result.parameterNames.isEmpty());
	}
	
	@Test
	public void withOneParameter() {
		String namedParameterStatementString = "SELECT * FROM CUSTOMER WHERE CUSTOMER_ID = :customerId";
		Result result = NamedParameteredStatementParser.parseNamedParameteredStatement(namedParameterStatementString);
		Assert.assertEquals("SELECT * FROM CUSTOMER WHERE CUSTOMER_ID = ?", result.parameteredStatementString);
		Assert.assertEquals(1, result.parameterNames.size());
		Assert.assertEquals("customerId", result.parameterNames.get(0));
	}
	
	@Test
	public void withTwoParameters() {
		String namedParameterStatementString = "SELECT * FROM CUSTOMER WHERE NAME = :name AND AGE = :age";
		Result result = NamedParameteredStatementParser.parseNamedParameteredStatement(namedParameterStatementString);
		Assert.assertEquals("SELECT * FROM CUSTOMER WHERE NAME = ? AND AGE = ?", result.parameteredStatementString);
		Assert.assertEquals(2, result.parameterNames.size());
		Assert.assertEquals("name", result.parameterNames.get(0));
		Assert.assertEquals("age", result.parameterNames.get(1));
	}
	
	@Test
	public void withTwoParametersWithSameName() {
		String namedParameterStatementString = "SELECT * FROM TRANSACTIONS WHERE SOURCE_WAREHOUSE_ID = :warehouseId OR TARGET_WAREHOUSE_ID = :warehouseId";
		Result result = NamedParameteredStatementParser.parseNamedParameteredStatement(namedParameterStatementString);
		Assert.assertEquals("SELECT * FROM TRANSACTIONS WHERE SOURCE_WAREHOUSE_ID = ? OR TARGET_WAREHOUSE_ID = ?", result.parameteredStatementString);
		Assert.assertEquals(2, result.parameterNames.size());
		Assert.assertEquals("warehouseId", result.parameterNames.get(0));
		Assert.assertEquals("warehouseId", result.parameterNames.get(1));
	}
	
	@Rule
	public ExpectedException expectedException = ExpectedException.none();
	
}
