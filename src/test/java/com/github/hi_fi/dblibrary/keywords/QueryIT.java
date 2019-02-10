package com.github.hi_fi.dblibrary.keywords;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.SQLException;

import org.junit.*;
import org.junit.rules.TemporaryFolder;

public class QueryIT {

	private Query query = new Query();
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

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	// ========================================================
	//
	// Check Execute SQL
	//
	// ========================================================

	@Test
	public void checkExecuteSQL() throws Exception {
		query.executeSql("CREATE TABLE TestTable (Num Integer)");
		asserter.tableMustExist("TESTTABLE");
	}

	// ========================================================
	//
	// Check Execute SQL From File
	//
	// ========================================================

	@Test
	public void checkExecuteSQLFromFileSingleLine() throws Exception {
		folder.getRoot();
		String myFileName = folder.getRoot().getPath() + File.separator + "myFile.sql";
		FileWriter fstream = new FileWriter(myFileName);
		BufferedWriter out = new BufferedWriter(fstream);
		out.write("CREATE TABLE TestTableESFFSL (Num Integer);\n");
		out.close();
		query.executeSqlFromFile(myFileName);
		asserter.tableMustExist("TESTTABLEESFFSL");
	}

	@Test
	public void checkExecuteSQLFromFileMultiLine() throws Exception {
		folder.getRoot();
		String myFileName = folder.getRoot().getPath() + File.separator + "myFile.sql";
		FileWriter fstream = new FileWriter(myFileName);
		BufferedWriter out = new BufferedWriter(fstream);
		out.write("CREATE TABLE TestTableESFFML (Num Integer);\n");
		out.write("INSERT INTO TestTableESFFML\n");
		out.write("VALUES(42);\n");
		out.close();
		query.executeSqlFromFile(myFileName);
		asserter.tableMustExist("TESTTABLEESFFML");
		asserter.checkContentForRowIdentifiedByWhereClause("Num", "42", "TestTableESFFML", "Num=42");
	}

	@Test
	public void checkExecuteSQLFromFileMultiLineComment() throws Exception {
		folder.getRoot();
		String myFileName = folder.getRoot().getPath() + File.separator + "myFile.sql";
		FileWriter fstream = new FileWriter(myFileName);
		BufferedWriter out = new BufferedWriter(fstream);
		out.write("-- Comment\n");
		out.write("CREATE TABLE TestTableESFFMLC;\n");
		out.close();
		query.executeSqlFromFile(myFileName);
		asserter.tableMustExist("TESTTABLEESFFMLC");
	}

	@Test
	public void checkExecuteSQLFromFileIgnoreErrorsSingleLine() throws Exception {
		folder.getRoot();
		String myFileName = folder.getRoot().getPath() + File.separator + "myFile.sql";
		FileWriter fstream = new FileWriter(myFileName);
		BufferedWriter out = new BufferedWriter(fstream);
		out.write("CREATE TABLE TestTableESFFIESL (Num Integer);\n");
		out.close();
		query.executeSqlFromFileIgnoreErrors(myFileName);
		asserter.tableMustExist("TESTTABLEESFFIESL");
	}

	@Test
	public void checkExecuteSQLFromFileIgnoreErrorsMultiLine() throws Exception {
		folder.getRoot();
		String myFileName = folder.getRoot().getPath() + File.separator + "myFile.sql";
		FileWriter fstream = new FileWriter(myFileName);
		BufferedWriter out = new BufferedWriter(fstream);
		out.write("CREATE TABLE TestTableESFFIEML (Num Integer);\n");
		out.write("INSERT INTO TestTableESFFIEML\n");
		out.write("VALUES(42);\n");
		out.close();
		query.executeSqlFromFileIgnoreErrors(myFileName);
		asserter.tableMustExist("TESTTABLEESFFIEML");
		asserter.checkContentForRowIdentifiedByWhereClause("Num", "42", "TESTTABLEESFFIEML", "Num=42");
	}

	@Test
	public void checkExecuteSQLFromFileIgnoreErrorsMultiLineComment() throws Exception {
		folder.getRoot();
		String myFileName = folder.getRoot().getPath() + File.separator + "myFile.sql";
		FileWriter fstream = new FileWriter(myFileName);
		BufferedWriter out = new BufferedWriter(fstream);
		out.write("-- Comment\n");
		out.write("CREATE TABLE TestTableESFFIEMLC;\n");
		out.close();
		query.executeSqlFromFileIgnoreErrors(myFileName);
		asserter.tableMustExist("TESTTABLEESFFIEMLC");
	}

	// ========================================================
	//
	// Check Read Single Value from Table
	//
	// ========================================================

	@Test
	public void checkReadSingleValueFromTable() throws Exception {
		String name = query.readSingleValueFromTable("MySampleTable", "Name", "id=1");
		System.out.println("Single Value Fetched: " + name);
		assertEquals("Wrong value fetched", "Donny Darko", name);
	}
	
	@Test
	public void checkReadSingleValueFromTableReturnsEmptyStringIfNoMatch() throws SQLException, DatabaseLibraryException {
		String name = query.readSingleValueFromTable("MySampleTable", "Name", "id=23");
		System.out.println("Single Value Fetched: " + name);
		assertEquals("Value found", "", name);
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
			query.deleteAllRowsFromTable("MySampleTable");
			asserter.tableMustBeEmpty("MySampleTable");
		}
	}		
}
