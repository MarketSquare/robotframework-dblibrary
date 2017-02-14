package com.github.hi_fi.dblibrary.keywords;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.github.hi_fi.dblibrary.keywords.DatabaseConnection;
import com.github.hi_fi.dblibrary.keywords.DatabaseLibraryException;

/**
 * Tests that use a database connection
 */
public class DatabaseLibraryTest {

	private DatabaseConnection databaseLibrary;
	private static Assert asserter;
	private static ConnectionHelper ch;
	

	// ========================================================
	//
	// Setup and Teardown on class-level
	//
	// ========================================================

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		asserter = new Assert();
		ch = new ConnectionHelper();
		ch.createTestDB();
	}
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		ch.deleteTestDB();
	}

	// ========================================================
	//
	// Setup and Teardown on method-level
	//
	// ========================================================
	
	@Before
	public void setUpTest() throws Exception {
		ch.initTestTables();
		databaseLibrary = ch.initDatabaseLibrary();
	}

	// ========================================================
	//
	// Check Delete all Rows from Table
	//
	// ========================================================

	@Test
	public void checkdelete_all_rows_from_table() throws Exception {
		try {
			// Check first that table is not empty
			asserter.tableMustBeEmpty("MySampleTable");
			fail();
		} catch (DatabaseLibraryException e) {
			databaseLibrary.deleteAllRowsFromTable("MySampleTable");
			asserter.tableMustBeEmpty("MySampleTable");
		}
	}

	// ========================================================
	//
	// Check Check Content for row identified by Rownum
	//
	// ========================================================

	@Test
	public void checkCheckContentIdentifiedbyRownum() throws Exception {
		databaseLibrary
				.checkContentForRowIdentifiedByRownum("Id,Name,Postings",
						"1|Donny Darko|1001", "MySampleTable", "1");
	}
	
	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckContentIdentifiedbyRownum_WrongValues() throws DatabaseLibraryException, Exception {
		databaseLibrary
				.checkContentForRowIdentifiedByRownum("Id,Name,Postings",
						"1|Donny Dar|1001", "MySampleTable", "1");
	}	
	
	
	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckContentIdentifiedbyRownum_NoRecordFound() throws DatabaseLibraryException, Exception {
		databaseLibrary
				.checkContentForRowIdentifiedByRownum("Id,Name,Postings",
						"1|Donny Dar|1001", "MySampleTable", "100");
	}		
	
	
	// ========================================================
	//
	// Check Check Content for row identified by where-clause
	//
	// ========================================================

	@Test
	public void checkCheckContentIdentifiedbyWhereClause() throws Exception {
		databaseLibrary
				.checkContentForRowIdentifiedByWhereClause("Id,Name,Postings",
						"1|Donny Darko|1001", "MySampleTable", "id=1");
	}	
	
	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckContentIdentifiedbyWhereClause_WrongValues() throws DatabaseLibraryException, Exception {
		databaseLibrary
				.checkContentForRowIdentifiedByWhereClause("Id,Name,Postings",
						"1|Donny Darko|100", "MySampleTable", "id=1");
	}	
	
	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckContentIdentifiedbyWhereClause_NoRecordFound() throws DatabaseLibraryException, Exception {
		databaseLibrary
				.checkContentForRowIdentifiedByWhereClause("Id,Name,Postings",
						"1|Donny Darko|100", "MySampleTable", "id=100");
	}	

	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckContentIdentifiedbyWhereClause_MoreThanOneRecordFound() throws DatabaseLibraryException, Exception {
		databaseLibrary
				.checkContentForRowIdentifiedByWhereClause("Id,Name,Postings",
						"1|Donny Darko|100", "MySampleTable", "id=1 or id=2");
	}	

	
	
	// ========================================================
	//
	// Check Transaction Isolation Level
	//
	// ========================================================

	@Test
	public void checkGetTransactionIsolationLevel() throws Exception {
		String level = databaseLibrary.getTransactionIsolationLevel();
		System.out.println("Transaction Isolation Level: " + level);
		
		if ((level == null) || (level.equals(ConnectionHelper.H2_PASSWORD))) {
			fail("Empty Transaction Isolation Level");
		}
	}	
	
	@Test
	public void checktransactionIsolationLevelMustBe() throws Exception {
		databaseLibrary.transactionIsolationLevelMustBe("TRANSACTION_READ_COMMITTED");
	}	
	
	@Test(expected=DatabaseLibraryException.class)
	public void checktransactionIsolationLevelMustBe_WithWrongLevelName() throws DatabaseLibraryException, Exception {
		databaseLibrary.transactionIsolationLevelMustBe("TRANSACTION_REPEATABLE_READ");
	}		
	
	
	// ========================================================
	//
	// Check Read Single Value from Table
	//
	// ========================================================

	@Test
	public void checkReadSingleValueFromTable() throws Exception {
		String name = databaseLibrary.readSingleValueFromTable("MySampleTable", "Name", "id=1");
		System.out.println("Single Value Fetched: " + name);
		assertEquals("Wrong value fetched", "Donny Darko", name);
	}
	
	@Test
	public void checkReadSingleValueFromTableReturnsEmptyStringIfNoMatch() throws SQLException, DatabaseLibraryException {
		String name = databaseLibrary.readSingleValueFromTable("MySampleTable", "Name", "id=23");
		System.out.println("Single Value Fetched: " + name);
		assertEquals("Value found", "", name);
	}

	
	// ========================================================
	//
	// Check Primary Key Column Information
	//
	// ========================================================

	@Test
	public void checkGetPrimaryKeyColumnsForTable() throws Exception {
		String keys = databaseLibrary.getPrimaryKeyColumnsForTable("MYSAMPLETABLE");
		System.out.println("Primary Keys: " + keys);
		
		if ((keys == null) || (keys.equals(ConnectionHelper.H2_PASSWORD))) {
			fail("Empty Primary Key");
		}
	}		
	
	@Test
	public void checkCheckPrimaryKeyColumnsForTable() throws Exception {
		databaseLibrary.checkPrimaryKeyColumnsForTable("MYSAMPLETABLE", "Id");
	}			

	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckPrimaryKeyColumnsForTable_NoMatch() throws DatabaseLibraryException, Exception {
		databaseLibrary.checkPrimaryKeyColumnsForTable("MYSAMPLETABLE", "Ids");
	}			
	
	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckPrimaryKeyColumnsForTable_WrongTableName() throws DatabaseLibraryException, Exception {
		databaseLibrary.checkPrimaryKeyColumnsForTable("WrongTable", "Id");
	}
	
	// Tests for "Row Should Not Exist In Table"
	@Test
	public void checkRowShouldNotExistInTable() throws SQLException, DatabaseLibraryException {
		databaseLibrary.rowShouldNotExistInTable("MySampleTable", "Name='John Doe'");
	}
	
	@Test
	public void checkRowShouldNotExistInTableFailsIfRowExists() throws SQLException, DatabaseLibraryException {
		try {
			databaseLibrary.rowShouldNotExistInTable("MySampleTable", "Name='Darth Vader'");
			fail();
		} catch(DatabaseLibraryException e) {
			assertEquals("Row exists (but should not) for where-clause: Name='Darth Vader' in table: MySampleTable", e.getMessage());
		}
	}

}
