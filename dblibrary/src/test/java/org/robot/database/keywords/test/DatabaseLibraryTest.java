package org.robot.database.keywords.test;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.hsqldb.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.robot.database.keywords.DatabaseLibrary;

public class DatabaseLibraryTest {

	private static Server hsqldbServer = null;

	
	//
	// Setup and Teardown on class-level
	//
	
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
				.execute("CREATE TABLE MySampleTable (Id Integer, Name VARCHAR(256), EMail VARCHAR(256), "
						+ "Postings Integer, State Integer, LastPosting Timestamp)");
		stmt.execute("CREATE TABLE EmptyTable (Id Integer, Name VARCHAR(256))");
		stmt.close();
		con.close();
	}

	@AfterClass
	public static void tearDown() throws Exception {
		hsqldbServer.shutdown();
	}

	
	//
	// Database Connection
	//
	
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

	
	//
	// Check Table Must be Empty
	//
	
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
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

	
	//
	// Check Table must Exists
	//
	
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
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
			
}
