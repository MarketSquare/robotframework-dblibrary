package com.github.hi_fi.dblibrary.keywords;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueryIT {

	private Query query = new Query();
	private Assert asserter = new Assert(); 
	

	// ========================================================
	//
	// Setup and Teardown on class-level
	//
	// ========================================================

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		new ConnectionHelper().createTestDB();
		new ConnectionHelper().initDatabaseLibrary();
	}
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		new ConnectionHelper().deleteTestDB();
	}

	//
	// Check Execute SQL
	//
	@Test
	public void checkExecuteSQL() throws Exception {
		query.executeSql("CREATE TABLE TestTable (Num Integer)");
		asserter.tableMustExist("TESTTABLE");
	}
}
