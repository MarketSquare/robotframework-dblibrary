package com.github.hi_fi.dblibrary.keywords;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AssertIT {


	private Assert asserter = new Assert();
	private static ConnectionHelper ch = new ConnectionHelper();
	

	// ========================================================
	//
	// Setup and Teardown on class-level
	//
	// ========================================================

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		ch.createTestDB();
		ch.initDatabaseLibrary();
	}
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		ch.deleteTestDB();
	}
	
	@Before
	public void setUpTest() throws Exception {
		ch.initTestTables();
		ch.initDatabaseLibrary();
	}

	// ========================================================
	//
	// Check Table Must be Empty
	//
	// ========================================================

	@Test
	public void checktable_must_be_empty_OnEmptyTable() throws Exception {
		asserter.tableMustBeEmpty("EmptyTable");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checktable_must_be_empty_OnTableNotEmpty() throws DatabaseLibraryException, Exception {
		asserter.tableMustBeEmpty("MySampleTable");
	}

	// ========================================================
	//
	// Check Table must Exists
	//
	// ========================================================

	@Test
	public void checkTableMustExist_ThatExists() throws Exception {
		asserter.tableMustExist("EMPTYTABLE");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checkTableMustExist_ThatDoesNotExist() throws DatabaseLibraryException, Exception {
		asserter.tableMustExist("WRONG_NAME");
	}
	
	// ========================================================
	//
	// Check Table Must contain less than number of rows
	//
	// ========================================================

	@Test
	public void checktable_must_contain_less_than_number_of_rows() throws Exception {
		asserter.tableMustContainLessThanNumberOfRows(
				"MySampleTable", "3");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checktable_must_contain_less_than_number_of_rows_SameNumbers() throws DatabaseLibraryException, Exception {
		asserter.tableMustContainLessThanNumberOfRows(
				"MySampleTable", "2");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checktable_must_contain_less_than_number_of_rows_LessNumbers() throws DatabaseLibraryException, Exception {
		asserter.tableMustContainLessThanNumberOfRows(
				"MySampleTable", "1");
	}
	
	// ========================================================
	//
	// Check Table Must contain more than number of rows
	//
	// ========================================================

	@Test
	public void checktable_must_contain_more_than_number_of_rows() throws Exception {
		asserter.tableMustContainMoreThanNumberOfRows(
				"MySampleTable", "1");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checktable_must_contain_more_than_number_of_rows_SameNumbers() throws DatabaseLibraryException, Exception {
		asserter.tableMustContainMoreThanNumberOfRows(
				"MySampleTable", "2");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checktable_must_contain_more_than_number_of_rows_MoreNumbers() throws Exception {
		asserter.tableMustContainMoreThanNumberOfRows(
				"MySampleTable", "200");
	}
	
	// ========================================================
	//
	// Check Table Must contain number of rows
	//
	// ========================================================

	@Test
	public void checktable_must_contain_number_of_rows() throws Exception {
		asserter.tableMustContainNumberOfRows("MySampleTable", "2");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checktable_must_contain_number_of_rows_WrongNumber() throws DatabaseLibraryException, Exception {
		asserter.tableMustContainNumberOfRows("MySampleTable", "5");
	}
	
	// ========================================================
	//
	// Check Tables Must contain same amount of rows
	//
	// ========================================================

	@Test
	public void checkTablesMustContainSameAmountOfRows() throws Exception {
		asserter.tablesMustContainSameAmountOfRows("MySampleTable",
				"ReferenceTable");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checkTablesMustContainSameAmountOfRows_ButTheyDoNot() throws DatabaseLibraryException, Exception {
		asserter.tablesMustContainSameAmountOfRows("MySampleTable",
				"EmptyTable");
	}
	
	
	@Test
	public void checkVerifyNumberOfRowsMatchingWhereSuccessOneMatch() throws Exception {
		asserter.verifyNumberOfRowsMatchingWhere("MySampleTable", "EMail='donny.darko@robot.org'", "1");
	}
	
	@Test
	public void checkVerifyNumberOfRowsMatchingWhereSuccessNoMatch() throws Exception {
		asserter.verifyNumberOfRowsMatchingWhere("MySampleTable", "EMail='batman@robot.org'", "0");
	}
	
	@Test(expected=DatabaseLibraryException.class)
	public void checkVerifyNumberOfRowsMatchingWhereFailure() throws DatabaseLibraryException, Exception {
		asserter.verifyNumberOfRowsMatchingWhere("MySampleTable", "Postings > 0", "1");
	}
}
