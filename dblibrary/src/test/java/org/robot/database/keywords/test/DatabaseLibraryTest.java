package org.robot.database.keywords.test;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Assert;

import org.hsqldb.Server;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.robot.database.keywords.DatabaseLibrary;
import org.robot.database.keywords.DatabaseLibraryException;

public class DatabaseLibraryTest {

	private static final String HSQL_DRIVER_CLASSNAME = "org.hsqldb.jdbcDriver";
	private static final String HSQL_URL = "jdbc:hsqldb:mem:xdb";
	private static final String HSQL_USER = "sa";
	private static final String HSQL_PASSWORD = "";

	private static Server hsqldbServer = null;
	
	private DatabaseLibrary databaseLibrary;

	// ========================================================
	//
	// Setup and Teardown on class-level
	//
	// ========================================================

	@BeforeClass
	public static void setUp() throws Exception {
		// Start the HSQLDB
		hsqldbServer = new Server();
		hsqldbServer.main(new String[] { "--database.0", "file:mydb",
				"--dbname.0", "xdb" });

		Class.forName(HSQL_DRIVER_CLASSNAME).newInstance();
		Connection con = DriverManager.getConnection(HSQL_URL,
				HSQL_USER, HSQL_PASSWORD);

		Statement stmt = con.createStatement();
		stmt
				.execute("CREATE TABLE MySampleTable (Id Integer NOT NULL, Name VARCHAR(256), "
						+ "EMail VARCHAR(256), "
						+ "Postings Integer, State Integer, LastPosting Timestamp)");
		stmt.execute("CREATE TABLE EmptyTable (Id Integer, Name VARCHAR(256))");
		stmt.execute("CREATE TABLE ReferenceTable (Num Integer)");

		stmt.execute("ALTER TABLE MySampleTable ADD CONSTRAINT MyUniqKey UNIQUE (Id)");
		stmt.execute("ALTER TABLE MySampleTable ADD CONSTRAINT MyPrimKey PRIMARY KEY (Id)");
		
		stmt.close();
		con.close();
	}

	@AfterClass
	public static void tearDown() throws Exception {
		hsqldbServer.shutdown();
	}

	// ========================================================
	//
	// Setup and Teardown on method-level
	//
	// ========================================================

	@Before
	public void setUpTest() throws Exception {
		initTestTables();
		initDatabaseLibrary();
	}
	
	public void initTestTables() throws Exception {
		Class.forName(HSQL_DRIVER_CLASSNAME).newInstance();
		Connection con = DriverManager.getConnection(HSQL_URL,
				HSQL_USER, HSQL_PASSWORD);

		Statement stmt = con.createStatement();
		stmt.execute("DELETE FROM MySampleTable");
		stmt.execute("DELETE FROM EmptyTable");
		stmt.execute("DELETE FROM ReferenceTable");
		stmt
				.executeUpdate("INSERT INTO MySampleTable VALUES(1, 'Donny Darko', "
						+ "'donny.darko@robot.org', 1001, 1, '2010-02-26 12:42:58.0000')");
		stmt
				.executeUpdate("INSERT INTO MySampleTable VALUES(2, 'Darth Vader', "
						+ "'darth.vader@starwars.universe', 123, 2, '2010-02-27 11:41:45.0000')");

		stmt.executeUpdate("INSERT INTO ReferenceTable VALUES(1)");
		stmt.executeUpdate("INSERT INTO ReferenceTable VALUES(2)");

		stmt.close();
		con.close();
	}
	
	private void initDatabaseLibrary() {
		databaseLibrary = new DatabaseLibrary();
	}

	// ========================================================
	//
	// Database Connection 
	//
	// ========================================================

	@Test
	public void checkConnectToDatabase() throws Exception {
		databaseLibrary.connectToDatabase(HSQL_DRIVER_CLASSNAME,
				HSQL_URL, HSQL_USER, HSQL_PASSWORD);
	}

	@Test
	public void checkConnectToDatabaseWithWrongUsername() {
		try {
			databaseLibrary.connectToDatabase(HSQL_DRIVER_CLASSNAME,
					HSQL_URL, "xyz", HSQL_PASSWORD);
		} catch (SQLException e) {
			if (!e.getMessage().contains("not found")) {
				e.printStackTrace();
				fail();
			}
		} catch (Exception e2) {
			e2.printStackTrace();
			fail();
		}
	}

	@Test
	public void checkDisconnectFromDatabase() throws Exception {
		databaseLibrary.connectToDatabase(HSQL_DRIVER_CLASSNAME,
				HSQL_URL, HSQL_USER, HSQL_PASSWORD);
		databaseLibrary.disconnectFromDatabase();
	}

	@Test(expected=IllegalStateException.class)
	public void checkIllegalStateExceptionWithoutConnect() throws Exception {
		databaseLibrary.tableMustBeEmpty("NoConnection");
	}
	
	
	// ========================================================
	//
	// Check Table Must be Empty
	//
	// ========================================================

	@Test
	public void checkTableMustBeEmpty_OnEmptyTable() throws Exception {
		connectToDatabase();

		databaseLibrary.tableMustBeEmpty("EmptyTable");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checkTableMustBeEmpty_OnTableNotEmpty() throws DatabaseLibraryException, Exception {
		connectToDatabase();

		databaseLibrary.tableMustBeEmpty("MySampleTable");
	}

	// ========================================================
	//
	// Check Table must Exists
	//
	// ========================================================

	@Test
	public void checkTableMustExist_ThatExists() throws Exception {
		connectToDatabase();

		databaseLibrary.tableMustExist("EMPTYTABLE");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checkTableMustExist_ThatDoesNotExist() throws DatabaseLibraryException, Exception {
		connectToDatabase();

		databaseLibrary.tableMustExist("WRONG_NAME");
	}

	// ========================================================
	//
	// Check Delete all Rows from Table
	//
	// ========================================================

	@Test
	public void checkDeleteAllRowsFromTable() throws Exception {
		connectToDatabase();

		try {
			// Check first that table is not empty
			databaseLibrary.tableMustBeEmpty("MySampleTable");
			fail();
		} catch (DatabaseLibraryException e) {
			databaseLibrary.deleteAllRowsFromTable("MySampleTable");
			databaseLibrary.tableMustBeEmpty("MySampleTable");
		}
	}

	// ========================================================
	//
	// Check Table Must contain number of rows
	//
	// ========================================================

	@Test
	public void checkTableMustContainNumberOfRows() throws Exception {
		connectToDatabase();

		databaseLibrary.tableMustContainNumberOfRows("MySampleTable", "2");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checkTableMustContainNumberOfRows_WrongNumber() throws DatabaseLibraryException, Exception {
		connectToDatabase();

		databaseLibrary.tableMustContainNumberOfRows("MySampleTable", "5");
	}

	// ========================================================
	//
	// Check Table Must contain more than number of rows
	//
	// ========================================================

	@Test
	public void checkTableMustContainMoreThanNumberOfRows() throws Exception {
		connectToDatabase();

		databaseLibrary.tableMustContainMoreThanNumberOfRows(
				"MySampleTable", "1");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checkTableMustContainMoreThanNumberOfRows_SameNumbers() throws DatabaseLibraryException, Exception {
		connectToDatabase();

		databaseLibrary.tableMustContainMoreThanNumberOfRows(
				"MySampleTable", "2");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checkTableMustContainMoreThanNumberOfRows_MoreNumbers() throws Exception {
		connectToDatabase();
		
		databaseLibrary.tableMustContainMoreThanNumberOfRows(
				"MySampleTable", "200");
	}

	// ========================================================
	//
	// Check Table Must contain less than number of rows
	//
	// ========================================================

	@Test
	public void checkTableMustContainLessThanNumberOfRows() throws Exception {
		connectToDatabase();

		databaseLibrary.tableMustContainLessThanNumberOfRows(
				"MySampleTable", "3");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checkTableMustContainLessThanNumberOfRows_SameNumbers() throws DatabaseLibraryException, Exception {
		connectToDatabase();

		databaseLibrary.tableMustContainLessThanNumberOfRows(
				"MySampleTable", "2");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checkTableMustContainLessThanNumberOfRows_LessNumbers() throws DatabaseLibraryException, Exception {
		connectToDatabase();

		databaseLibrary.tableMustContainLessThanNumberOfRows(
				"MySampleTable", "1");
	}

	// ========================================================
	//
	// Check Tables Must contain same amount of rows
	//
	// ========================================================

	@Test
	public void checkTablesMustContainSameAmountOfRows() throws Exception {
		connectToDatabase();

		databaseLibrary.tablesMustContainSameAmountOfRows("MySampleTable",
				"ReferenceTable");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checkTablesMustContainSameAmountOfRows_ButTheyDoNot() throws DatabaseLibraryException, Exception {
		connectToDatabase();

		databaseLibrary.tablesMustContainSameAmountOfRows("MySampleTable",
				"EmptyTable");
	}

	// ========================================================
	//
	// Check Check Content for row identified by Rownum
	//
	// ========================================================

	@Test
	public void checkCheckContentIdentifiedbyRownum() throws Exception {
		connectToDatabase();

		databaseLibrary
				.checkContentForRowIdentifiedByRownum("Id,Name,Postings",
						"1|Donny Darko|1001", "MySampleTable", "1");
	}
	
	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckContentIdentifiedbyRownum_WrongValues() throws DatabaseLibraryException, Exception {
		connectToDatabase();

		databaseLibrary
				.checkContentForRowIdentifiedByRownum("Id,Name,Postings",
						"1|Donny Dar|1001", "MySampleTable", "1");
	}	
	
	
	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckContentIdentifiedbyRownum_NoRecordFound() throws DatabaseLibraryException, Exception {
		connectToDatabase();

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
		connectToDatabase();

		databaseLibrary
				.checkContentForRowIdentifiedByWhereClause("Id,Name,Postings",
						"1|Donny Darko|1001", "MySampleTable", "id=1");
	}	
	
	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckContentIdentifiedbyWhereClause_WrongValues() throws DatabaseLibraryException, Exception {
		connectToDatabase();

		databaseLibrary
				.checkContentForRowIdentifiedByWhereClause("Id,Name,Postings",
						"1|Donny Darko|100", "MySampleTable", "id=1");
	}	
	
	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckContentIdentifiedbyWhereClause_NoRecordFound() throws DatabaseLibraryException, Exception {
		connectToDatabase();
		
		databaseLibrary
				.checkContentForRowIdentifiedByWhereClause("Id,Name,Postings",
						"1|Donny Darko|100", "MySampleTable", "id=100");
	}	

	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckContentIdentifiedbyWhereClause_MoreThanOneRecordFound() throws DatabaseLibraryException, Exception {
		connectToDatabase();

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
		connectToDatabase();

		String level = databaseLibrary.getTransactionIsolationLevel();
		System.out.println("Transaction Isolation Level: " + level);
		
		if ((level == null) || (level.equals(HSQL_PASSWORD))) {
			fail("Empty Transaction Isolation Level");
		}
	}	
	
	@Test
	public void checktransactionIsolationLevelMustBe() throws Exception {
		connectToDatabase();

		databaseLibrary.transactionIsolationLevelMustBe("TRANSACTION_READ_COMMITTED");
	}	
	
	@Test(expected=DatabaseLibraryException.class)
	public void checktransactionIsolationLevelMustBe_WithWrongLevelName() throws DatabaseLibraryException, Exception {
		connectToDatabase();

		databaseLibrary.transactionIsolationLevelMustBe("TRANSACTION_REPEATABLE_READ");
	}		
	
	
	// ========================================================
	//
	// Check Read Single Value from Table
	//
	// ========================================================

	@Test
	public void checkReadSingleValueFromTable() throws Exception {
		connectToDatabase();

		String name = databaseLibrary.readSingleValueFromTable("MySampleTable", "Name", "id=1");
		System.out.println("Single Value Fetched: " + name);
		Assert.assertEquals("Wrong value fetched", "Donny Darko", name);
	}	

	
	// ========================================================
	//
	// Check Primary Key Column Information
	//
	// ========================================================

	@Test
	public void checkGetPrimaryKeyColumnsForTable() throws Exception {
		connectToDatabase();

		String keys = databaseLibrary.getPrimaryKeyColumnsForTable("MYSAMPLETABLE");
		System.out.println("Primary Keys: " + keys);
		
		if ((keys == null) || (keys.equals(HSQL_PASSWORD))) {
			fail("Empty Primary Key");
		}
	}		
	
	@Test
	public void checkCheckPrimaryKeyColumnsForTable() throws Exception {
		connectToDatabase();

		databaseLibrary.checkPrimaryKeyColumnsForTable("MYSAMPLETABLE", "Id");
	}			

	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckPrimaryKeyColumnsForTable_NoMatch() throws DatabaseLibraryException, Exception {
		connectToDatabase();

		databaseLibrary.checkPrimaryKeyColumnsForTable("MYSAMPLETABLE", "Ids");
		fail();
	}			
	
	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckPrimaryKeyColumnsForTable_WrongTableName() throws DatabaseLibraryException, Exception {
		connectToDatabase();

		databaseLibrary.checkPrimaryKeyColumnsForTable("WrongTable", "Id");
	}
	
	
	//
	// Check Execute SQL
	//
	@Test
	public void checkExecuteSQL() throws Exception {
		connectToDatabase();

		databaseLibrary.executeSQL("CREATE TABLE TestTable (Num Integer)");
		databaseLibrary.tableMustExist("TESTTABLE");
	}
	
	@Test
	public void checkVerifyNumberOfRowsMatchingWhereSuccessOneMatch() throws Exception {
		connectToDatabase();
		
		databaseLibrary.verifyNumberOfRowsMatchingWhere("MySampleTable", "EMail='donny.darko@robot.org'", "1");
	}
	
	@Test
	public void checkVerifyNumberOfRowsMatchingWhereSuccessNoMatch() throws Exception {
		connectToDatabase();
		
		databaseLibrary.verifyNumberOfRowsMatchingWhere("MySampleTable", "EMail='batman@robot.org'", "0");
	}
	
	@Test(expected=DatabaseLibraryException.class)
	public void checkVerifyNumberOfRowsMatchingWhereFailure() throws DatabaseLibraryException, Exception {
		connectToDatabase();
		
		databaseLibrary.verifyNumberOfRowsMatchingWhere("MySampleTable", "Postings > 0", "1");
	}
	
	// Utility methods to clean up the test cases
	
	private void connectToDatabase() {
		try {
			databaseLibrary.connectToDatabase(HSQL_DRIVER_CLASSNAME,
					HSQL_URL, HSQL_USER, HSQL_PASSWORD);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
}
