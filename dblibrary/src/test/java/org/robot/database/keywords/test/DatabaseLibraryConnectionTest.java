package org.robot.database.keywords.test;

import static org.junit.Assert.fail;

import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;
import org.robot.database.keywords.DatabaseLibrary;

/**
 * Tests related to connecting to the database
 */
public class DatabaseLibraryConnectionTest {

	private static final String HSQL_DRIVER_CLASSNAME = "org.hsqldb.jdbcDriver";
	private static final String HSQL_URL = "jdbc:hsqldb:mem:xdb";
	private static final String HSQL_USER = "sa";
	private static final String HSQL_PASSWORD = "";

	private DatabaseLibrary databaseLibrary;

	@Before
	public void setUpTest() throws Exception {
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

}
