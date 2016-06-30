package com.github.hi_fi.dblibrary.keywords;

import static org.junit.Assert.fail;

import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import com.github.hi_fi.dblibrary.keywords.DatabaseLibrary;

/**
 * Tests related to connecting to the database
 */
public class DatabaseLibraryConnectionTest {

	private static final String H2_DRIVER_CLASSNAME = "org.h2.Driver";
	private static final String H2_URL = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
	private static final String H2_USER = "sa";
	private static final String H2_PASSWORD = "";

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
		databaseLibrary.connectToDatabase(H2_DRIVER_CLASSNAME,
				H2_URL, H2_USER, H2_PASSWORD);
	}

	@Test
	public void checkConnectToDatabaseWithWrongUsername() {
		try {
			databaseLibrary.connectToDatabase(H2_DRIVER_CLASSNAME,
					H2_URL, "xyz", H2_PASSWORD);
		} catch (SQLException e) {
			if (!e.getMessage().contains("Wrong user name or password")) {
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
		databaseLibrary.connectToDatabase(H2_DRIVER_CLASSNAME,
				H2_URL, H2_USER, H2_PASSWORD);
		databaseLibrary.disconnectFromDatabase();
	}

	@Test(expected=IllegalStateException.class)
	public void checkIllegalStateExceptionWithoutConnect() throws Exception {
		databaseLibrary.tableMustBeEmpty("NoConnection");
	}

}
