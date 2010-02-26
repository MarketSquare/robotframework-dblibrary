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

	private static Server hsqldbServer = null;

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

		Class.forName("org.hsqldb.jdbcDriver").newInstance();
		Connection con = DriverManager.getConnection("jdbc:hsqldb:mem:xdb",
				"sa", "");

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
	public void initTestTables() throws Exception {
		Class.forName("org.hsqldb.jdbcDriver").newInstance();
		Connection con = DriverManager.getConnection("jdbc:hsqldb:mem:xdb",
				"sa", "");

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

	// ========================================================
	//
	// Database Connection
	//
	// ========================================================

	@Test
	public void checkConnectToDatabase() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void checkConnectToDatabaseWithWrongUsername() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "xyz", "");
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

	// ========================================================
	//
	// Check Table Must be Empty
	//
	// ========================================================

	@Test
	public void checkTableMustBeEmpty_OnEmptyTable() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary.tableMustBeEmpty("EmptyTable");
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void checkTableMustBeEmpty_OnTableNotEmpty() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary.tableMustBeEmpty("MySampleTable");
			fail();
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e) {
			// This Exception is expected as the test fails
		}
	}

	// ========================================================
	//
	// Check Table must Exists
	//
	// ========================================================

	@Test
	public void checkTableMustExist_ThatExists() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary.tableMustExist("EMPTYTABLE");
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void checkTableMustExist_ThatDoesNotExist() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary.tableMustExist("WRONG_NAME");
			fail();
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e) {
			// This Exception is expected as the test fails
		}
	}

	// ========================================================
	//
	// Check Delete all Rows from Table
	//
	// ========================================================

	@Test
	public void checkDeleteAllRowsFromTable() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			// Check first that table is not empty
			databaseLibrary.tableMustBeEmpty("MySampleTable");
			fail();
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e) {
			try {
				databaseLibrary.deleteAllRowsFromTable("MySampleTable");
				databaseLibrary.tableMustBeEmpty("MySampleTable");
			} catch (SQLException e1) {
				e1.printStackTrace();
				fail();
			} catch (DatabaseLibraryException e2) {
				e2.printStackTrace();
				fail();
			}

		}
	}

	// ========================================================
	//
	// Check Table Must contain number of rows
	//
	// ========================================================

	@Test
	public void checkTableMustContainNumberOfRows() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary.tableMustContainNumberOfRows("MySampleTable", 2);
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e2) {
			e2.printStackTrace();
			fail();
		}
	}

	@Test
	public void checkTableMustContainNumberOfRows_WrongNumber() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary.tableMustContainNumberOfRows("MySampleTable", 5);
			fail();
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e2) {
			// This Exception is expected as the test fails
		}
	}

	// ========================================================
	//
	// Check Table Must contain more than number of rows
	//
	// ========================================================

	@Test
	public void checkTableMustContainMoreThanNumberOfRows() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary.tableMustContainMoreThanNumberOfRows(
					"MySampleTable", 1);
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e2) {
			e2.printStackTrace();
			fail();
		}
	}

	@Test
	public void checkTableMustContainMoreThanNumberOfRows_SameNumbers() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary.tableMustContainMoreThanNumberOfRows(
					"MySampleTable", 2);
			fail();
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e2) {
			// This Exception is expected as the test fails
		}
	}

	@Test
	public void checkTableMustContainMoreThanNumberOfRows_MoreNumbers() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary.tableMustContainMoreThanNumberOfRows(
					"MySampleTable", 200);
			fail();
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e2) {
			// This Exception is expected as the test fails
		}
	}

	// ========================================================
	//
	// Check Table Must contain less than number of rows
	//
	// ========================================================

	@Test
	public void checkTableMustContainLessThanNumberOfRows() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary.tableMustContainLessThanNumberOfRows(
					"MySampleTable", 3);
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e2) {
			e2.printStackTrace();
			fail();
		}
	}

	@Test
	public void checkTableMustContainLessThanNumberOfRows_SameNumbers() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary.tableMustContainLessThanNumberOfRows(
					"MySampleTable", 2);
			fail();
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e2) {
			// This Exception is expected as the test fails
		}
	}

	@Test
	public void checkTableMustContainLessThanNumberOfRows_LessNumbers() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary.tableMustContainLessThanNumberOfRows(
					"MySampleTable", 1);
			fail();
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e2) {
			// This Exception is expected as the test fails
		}
	}

	// ========================================================
	//
	// Check Tables Must contain same amount of rows
	//
	// ========================================================

	@Test
	public void checkTablesMustContainSameAmountOfRows() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary.tablesMustContainSameAmountOfRows("MySampleTable",
					"ReferenceTable");
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e2) {
			e2.printStackTrace();
			fail();
		}
	}

	@Test
	public void checkTablesMustContainSameAmountOfRows_ButTheyDoNot() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary.tablesMustContainSameAmountOfRows("MySampleTable",
					"EmptyTable");
			fail();
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e2) {
			// This Exception is expected as the test fails
		}
	}

	// ========================================================
	//
	// Check Check Content for row identified by Rownum
	//
	// ========================================================

	@Test
	public void checkCheckContentIdentifiedbyRownum() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary
					.checkContentForRowIdentifiedByRownum("Id,Name,Postings",
							"1|Donny Darko|1001", "MySampleTable", 1);
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e2) {
			e2.printStackTrace();
			fail();
		}
	}
	
	@Test
	public void checkCheckContentIdentifiedbyRownum_WrongValues() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary
					.checkContentForRowIdentifiedByRownum("Id,Name,Postings",
							"1|Donny Dar|1001", "MySampleTable", 1);
			fail();
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e2) {
			// This Exception is expected as the test fails
		}
	}	
	
	
	@Test
	public void checkCheckContentIdentifiedbyRownum_NoRecordFound() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary
					.checkContentForRowIdentifiedByRownum("Id,Name,Postings",
							"1|Donny Dar|1001", "MySampleTable", 100);
			fail();
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e2) {
			// This Exception is expected as the test fails
		}
	}		
	
	
	// ========================================================
	//
	// Check Check Content for row identified by where-clause
	//
	// ========================================================

	@Test
	public void checkCheckContentIdentifiedbyWhereClause() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary
					.checkContentForRowIdentifiedByWhereClause("Id,Name,Postings",
							"1|Donny Darko|1001", "MySampleTable", "id=1");
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e2) {
			e2.printStackTrace();
			fail();
		}
	}	
	
	@Test
	public void checkCheckContentIdentifiedbyWhereClause_WrongValues() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary
					.checkContentForRowIdentifiedByWhereClause("Id,Name,Postings",
							"1|Donny Darko|100", "MySampleTable", "id=1");
			fail();
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e2) {
			// This Exception is expected as the test fails
		}
	}	
	
	@Test
	public void checkCheckContentIdentifiedbyWhereClause_NoRecordFound() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
		
		try {
			databaseLibrary
					.checkContentForRowIdentifiedByWhereClause("Id,Name,Postings",
							"1|Donny Darko|100", "MySampleTable", "id=100");
			fail();
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e2) {
			// This Exception is expected as the test fails
		}
	}	

	@Test
	public void checkCheckContentIdentifiedbyWhereClause_MoreThanOneRecordFound() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary
					.checkContentForRowIdentifiedByWhereClause("Id,Name,Postings",
							"1|Donny Darko|100", "MySampleTable", "id=1 or id=2");
			fail();
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e2) {
			// This Exception is expected as the test fails
		}
	}	

	
	
	// ========================================================
	//
	// Check Transaction Isolation Level
	//
	// ========================================================

	@Test
	public void checkGetTransactionIsolationLevel() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			String level = databaseLibrary.getTransactionIsolationLevel();
			System.out.println("Transaction Isolation Level: " + level);
			
			if ((level == null) || (level.equals(""))) {
				fail("Empty Transaction Isolation Level");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} 
	}	
	
	@Test
	public void checktransactionIsolationLevelMustBe() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary.transactionIsolationLevelMustBe("TRANSACTION_READ_COMMITTED");
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e) {
			e.printStackTrace();
			fail();
		} 
	}	
	
	@Test
	public void checktransactionIsolationLevelMustBe_WithWrongLevelName() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary.transactionIsolationLevelMustBe("TRANSACTION_REPEATABLE_READ");
			fail();
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e) {
			// This Exception is expected as the test fails
		} 
	}		
	
	
	// ========================================================
	//
	// Check Read Single Value from Table
	//
	// ========================================================

	@Test
	public void checkReadSingleValueFromTable() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			String name = databaseLibrary.readSingleValueFromTable("MySampleTable", "Name", "id=1");
			System.out.println("Single Value Fetched: " + name);
			Assert.assertEquals("Wrong value fetched", "Donny Darko", name);
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} 
	}	

	
	// ========================================================
	//
	// Check Primary Key Column Information
	//
	// ========================================================

	@Test
	public void checkGetPrimaryKeyColumnsForTable() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			String keys = databaseLibrary.getPrimaryKeyColumnsForTable("MYSAMPLETABLE");
			System.out.println("Primary Keys: " + keys);
			
			if ((keys == null) || (keys.equals(""))) {
				fail("Empty Primary Key");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} 
	}		
	
	@Test
	public void checkCheckPrimaryKeyColumnsForTable() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary.checkPrimaryKeyColumnsForTable("MYSAMPLETABLE", "Id");
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e) {
			e.printStackTrace();
			fail();
		} 
	}			

	@Test
	public void checkCheckPrimaryKeyColumnsForTable_NoMatch() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary.checkPrimaryKeyColumnsForTable("MYSAMPLETABLE", "Ids");
			fail();
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e) {
			// This Exception is expected as the test fails
		} 
	}			
	
	@Test
	public void checkCheckPrimaryKeyColumnsForTable_WrongTableName() {
		DatabaseLibrary databaseLibrary = new DatabaseLibrary();
		try {
			databaseLibrary.connectToDatabase("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:mem:xdb", "sa", "");
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			databaseLibrary.checkPrimaryKeyColumnsForTable("WrongTable", "Id");
			fail();
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		} catch (DatabaseLibraryException e) {
			// This Exception is expected as the test fails
		} 
	}				
	
}
