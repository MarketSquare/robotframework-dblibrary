package org.robot.database.keywords;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.robotframework.javalib.annotation.ArgumentNames;
import org.robotframework.javalib.annotation.RobotKeyword;
import org.robotframework.javalib.annotation.RobotKeywords;

/**
 * This library supports database-related testing using the Robot Framework. It
 * allows to establish a connection to a certain database to perform tests on
 * the content of certain tables and/or views in that database. A possible
 * scenario for its usage is a Web-Application that is storing data to the
 * database based on some user actions (probably a quite common scenario). The
 * actions in the Web-Application could be triggered using some tests based on
 * Selenium and in the same test it will then be possible to check if the proper
 * data has ended up in the database as expected. Of course there are various
 * other scenarios where this library might be used.
 * 
 * As this library is written in Java support for a lot of different database
 * systems is possible. This only requires the corresponding driver-classes
 * (usually in the form of a JAR from the database provider) and the knowledge
 * of a proper JDBC connection-string.
 * 
 * The following table lists some examples of drivers and connection strings
 * for some popular databases. 
 * | *Database* | *Driver Name* | *Sample Connection String* | *Download Driver* |
 * | MySql | com.mysql.jdbc.Driver | jdbc:mysql://servername/dbname | http://dev.mysql.com/downloads/connector/j/ |
 * | Oracle | oracle.jdbc.driver.OracleDriver | jdbc:oracle:thin:@servername:port:dbname | http://www.oracle.com/technology/tech/java/sqlj_jdbc/htdocs/jdbc_faq.html |
 * 
 * The examples in the description of the keywords is based on a database table
 * named "MySampleTable" that has the following layout:
 * 
 * MySampleTable: 
 * | *COLUMN* | *TYPE* |
 * | Id | Number |
 * | Name | String | 
 * | EMail | String |
 * | Postings | Number | 
 * | State | Number | 
 * | LastPosting | Timestamp |
 * 
 * NOTE: A lot of keywords that are targeted for Tables will work equally with
 * Views as this is often no difference if Select-statements are performed.
 * 
 * *Remote Library Support*
 * Sine release v2.0 Database Library supports the Remote Server Interface of the Robot Framework.
 * This means you can start the Library as an own Server and use the provided keywords remotely.
 * Especially for libraries written in Java (like this one) this has the big advantage that you 
 * can still use pybot to start the Robot Framework (no Jython required) and still use these keywords
 * provided as a Java Library.
 * 
 * Please take a look at the sample project here (https://github.com/ThomasJaspers/robotframework-dblibrary/wiki/Database-Library-Sample) 
 * to see how the Remote Server functionality can be utilized in your testsuites.
 * 
 */
public class DatabaseLibrary {
	public static final String ROBOT_LIBRARY_SCOPE = "GLOBAL";

	private Connection connection = null;

	public static final Map<String, String> documentation;

	static {
        Map<String, String> map = new HashMap<String, String>();
        
        map.put("connect_to_database", "Connects to the database defined by the given arguments.\n\n"
    			+ "Example:\n"
    			+ "| Connect To Database | com.mysql.jdbc.Driver | jdbc:mysql://my.host.name/myinstance | UserName | ThePassword |\n");
        map.put("disconnect_from_database", "Disconnects from the database.\n\n" + "Example:\n"
        		+ "| Disconnect from Database |\n");
        map.put("table_must_exist", "Checks that the given table exists.\n\n" + "Example:\n"
    			+ "| Table Must Exist | MySampleTable |\n"); 
        map.put("table_must_be_empty", "Checks that the given table is empty.\n\n" + "Example:\n"
    			+ "| Table Must Be Empty | MySampleTable |\n"); 
        map.put("delete_all_rows_from_table", "Deletes all rows from the given table.\n\n" + "Example:\n"
			+ "| Delete All Rows From Table | MySampleTable |\n");
        map.put("table_must_contain_number_of_rows", "Checks that the given table contains the given number of records.\n\n"
    			+ "Example:\n"
    			+ "| Table Must Contain Number Of Rows | MySampleTable | 14 |\n"); 
        map.put("table_must_contain_more_than_number_of_rows", "Checks that the given table contains more than the given number of records.\n\n"
    			+ "Example:\n"
    			+ "| Table Must Contain More Than Number Of Rows | MySampleTable | 99 |\n"); 
        map.put("table_must_contain_less_than_number_of_rows", "Checks that the given table contains less than the given number of records.\n\n"
    			+ "Example:\n"
    			+ "| Table Must Contain Less Than Number Of Rows | MySampleTable | 99 |\n");
        map.put("tables_must_contain_same_amount_of_rows", "Checks that the given tables contain the same amount of rows.\n\n"
        		+ "Example:\n" 
        		+ "| Tables Must Contain Same Amount Of Rows | MySampleTable | MyCompareTable |\n");
        map.put("check_content_for_row_identified_by_rownum", "Checks for a given value in the selected row.\n\n"
        		+ "Example:\n" 
        		+ "| Check Content for Row Identified by Rownum | Name,EMail | John Doe|john.doe@x-files | MySampleTable | 4 |\n");
        map.put("check_content_for_row_identified_by_where_clause", "Checks for a given value in the selected row.\n\n"
        		+ "Example:\n" 
        		+ "| Check Content for Row Identified by WhereClause | Name,EMail | John Doe|john.doe@x-files | MySampleTable | Postings=14 |\n"); 
        map.put("read_single_value_from_table", "Reads and returns the defined value from the given table.\n\n"
        		+ "Example:\n" 
        		+ "| ${VALUE}= | Read single Value from Table | MySampleTable | EMail | Name='John Doe' |\n");
        map.put("transaction_isolation_level_must_be", "Checks that the given transaction level is set.\n\n"
        		+ "Example:"
        		+ "| Transaction Isolation Level Must Be | TRANSACTION_READ_COMMITTED |\n");
        map.put("get_transaction_isolation_level", "Returns the transaction level set.\n\n"
        		+ "Example:\n"
        		+ "| ${TI_LEVEL}= | Get Transaction Isolation Level |\n");
        map.put("check_primary_key_columns_for_table", "Checks for the expected primary key.\n\n"
        		+ "Example:\n" 
        		+ "| Check Primary Key Columns For Table | MySampleTable | Id,Name |\n");
    	map.put("get_primary_key_columns_for_table", "Returns the primary key column names for the given table.\n\n"
    			+ "Example:\n" 
    			+ "| ${KEYS}= | Get Primary Key Columns For Table | MySampleTable |\n");
    	map.put("execute_sql", "Executes the given SQL statement.\n\n"
    			+ "Example:\n" 
    			+ "| Execute SQL | CREATE TABLE MyTable (Num INTEGER) |\n");
    	map.put("execute_sql_from_file", "Executes the SQL statements from the given file.\n\n"
    			+ "Example:" 
    			+ "| Execute SQL from File | myFile.sql |\n");
    	map.put("execute_sql_from_file_ignore_errors", "Executes the SQL statements from the given file not stopping on errors.\n\n"
    			+ "Example:" 
    			+ "| Execute SQL from File | myFile.sql |\n");
    	map.put("verify_number_of_rows_matching_where", "Checks that a given table contains a given amount of rows matching a given WHERE clause.\n\n"
    			+ "Example:" 
    			+ "| Verify Number Of Rows Matching Where | MySampleTable | email=x@y.net | 2 |\n\n");
    	map.put("row_should_not_exist_in_table", "Checks that the row defined by the where-clause does not exist in the given table.\n\n" 
    			+ "Example:\n"
    			+ "| Row Should Not Exist In Table | MySampleTable | Name='John Doe' |\n\n");
        
        documentation = Collections.unmodifiableMap(map);
    }

	
	/**
	 * Default-constructor
	 */
	public DatabaseLibrary() {
	}

	/**
	 * Establish the connection to the database. This is mandatory before any of
	 * the other keywords can be used and should be ideally done during the
	 * suite setup phase. To avoid problems ensure to close the connection again
	 * using the disconnect-keyword.
	 * 
	 * It must be ensured that the JAR-file containing the given driver can be
	 * found from the CLASSPATH when starting robot. Furthermore it must be
	 * noted that the connection string is database-specific and must be valid
	 * of course.
	 * 
	 * Example: 
	 * | Connect To Database | com.mysql.jdbc.Driver | jdbc:mysql://my.host.name/myinstance | UserName | ThePassword |
	 * 
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * 
	 */
	public void connectToDatabase(String driverClassName, String connectString,
			String dbUser, String dbPassword) throws SQLException,
			InstantiationException, IllegalAccessException,
			ClassNotFoundException {

		Class.forName(driverClassName).newInstance();
		setConnection(DriverManager.getConnection(connectString, dbUser,
				dbPassword));
	}

	/**
	 * Releases the existing connection to the database. In addition this
	 * keyword will log any SQLWarnings that might have been occurred on the
	 * connection.
	 * 
	 * Example: 
	 * | Disconnect from Database |
	 */
	public void disconnectFromDatabase() throws SQLException {
		System.out.println("SQL Warnings on this connection: "
				+ getConnection().getWarnings());
		getConnection().close();
	}

	/**
	 * Checks that a table with the given name exists. If the table does not
	 * exist the test will fail.
	 * 
	 * NOTE: Some database expect the table names to be written all in upper
	 * case letters to be found.
	 * 
	 * Example: 
	 * | Table Must Exist | MySampleTable |
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void tableMustExist(String tableName) throws SQLException,
			DatabaseLibraryException {

		DatabaseMetaData dbm = getConnection().getMetaData();
		ResultSet rs = dbm.getTables(null, null, tableName, null);
		try {
			if (!rs.next()) {
				throw new DatabaseLibraryException("Table: " + tableName
						+ " was not found");
			}
		} finally {
			rs.close();
		}
	}

	/**
	 * Checks that the given table has no rows. It is a convenience way of using
	 * the "Table Must Contain Number Of Rows" with zero for the amount of rows.
	 * 
	 * Example: 
	 * | Table Must Be Empty | MySampleTable |
	 * 
	 * @throws DatabaseLibraryException
	 * @throws SQLException
	 */
	public void tableMustBeEmpty(String tableName) throws SQLException,
			DatabaseLibraryException {
		tableMustContainNumberOfRows(tableName, "0");
	}

	/**
	 * Deletes the entire content of the given database table. This keyword is
	 * useful to start tests in a clean state. Use this keyword with care as
	 * accidently execution of this keyword in a productive system will cause
	 * heavy loss of data. There will be no rollback possible.
	 * 
	 * Example: 
	 * | Delete All Rows From Table | MySampleTable |
	 * 
	 * @throws SQLException
	 */
	public void deleteAllRowsFromTable(String tableName) throws SQLException {
		String sql = "delete from " + tableName;

		Statement stmt = getConnection().createStatement();
		try {
			stmt.execute(sql);
		} finally {
			stmt.close();
		}
	}

	/**
	 * This keyword checks that a given table contains a given amount of rows.
	 * For the example this means that the table "MySampleTable" must contain
	 * exactly 14 rows, otherwise the teststep will fail.
	 * 
	 * Example: 
	 * | Table Must Contain Number Of Rows | MySampleTable | 14 |
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void tableMustContainNumberOfRows(String tableName,
			String rowNumValue) throws SQLException, DatabaseLibraryException {

		long rowNum = Long.valueOf(rowNumValue);

		long num = getNumberOfRows(tableName, rowNum + 1);
		if (num != rowNum) {
			throw new DatabaseLibraryException("Expecting " + rowNum
					+ " rows, fetched: " + num);
		}
	}

	/**
	 * This keyword checks that a given table contains more than the given
	 * amount of rows. For the example this means that the table "MySampleTable"
	 * must contain 100 or more rows, otherwise the teststep will fail.
	 * 
	 * Example: 
	 * | Table Must Contain More Than Number Of Rows | MySampleTable | 99 |
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void tableMustContainMoreThanNumberOfRows(String tableName,
			String rowNumValue) throws SQLException, DatabaseLibraryException {

		long rowNum = Long.valueOf(rowNumValue);

		long num = getNumberOfRows(tableName, rowNum + 1);
		if (num <= rowNum) {
			throw new DatabaseLibraryException("Expecting more than" + rowNum
					+ " rows, fetched: " + num);
		}
	}

	/**
	 * This keyword checks that a given table contains less than the given
	 * amount of rows. For the example this means that the table "MySampleTable"
	 * must contain anything between 0 and 1000 rows, otherwise the teststep
	 * will fail.
	 * 
	 * Example: 
	 * | Table Must Contain Less Than Number Of Rows | MySampleTable | 1001 |
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void tableMustContainLessThanNumberOfRows(String tableName,
			String rowNumValue) throws SQLException, DatabaseLibraryException {

		long rowNum = Long.valueOf(rowNumValue);

		long num = getNumberOfRows(tableName, rowNum);
		if (num >= rowNum) {
			throw new DatabaseLibraryException("Expecting less than" + rowNum
					+ " rows, fetched: " + num);
		}
	}

	/**
	 * This keyword checks that two given database tables have the same amount
	 * of rows.
	 * 
	 * Example: 
	 * | Tables Must Contain Same Amount Of Rows | MySampleTable | MyCompareTable |
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void tablesMustContainSameAmountOfRows(String firstTableName,
			String secondTableName) throws SQLException,
			DatabaseLibraryException {

		long firstNum = getNumberOfRows(firstTableName);
		long secondNum = getNumberOfRows(secondTableName);

		if (firstNum != secondNum) {
			throw new DatabaseLibraryException(
					"Expecting same amount of rows, but table "
							+ firstTableName + " has " + firstNum
							+ " rows and table " + secondTableName + " has "
							+ secondNum + " rows!");
		}
	}

	/**
	 * This keyword can be used to check for proper content inside a specific
	 * row in a database table. For this it is possible to give a
	 * comma-separated list of column names in the first parameter and a
	 * pipe-separated list of values in the second parameter. Then the name of
	 * the table and the rownum to check must be passed to this keyword. The
	 * corresponding values are then read from that row in the given table and
	 * compared to the expected values. If all values match the teststep will
	 * pass, otherwise it will fail.
	 * 
	 * Example: 
	 * | Check Content for Row Identified by Rownum | Name,EMail | John Doe|john.doe@x-files | MySampleTable | 4 |
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void checkContentForRowIdentifiedByRownum(String columnNames,
			String expectedValues, String tableName, String rowNumValue)
			throws SQLException, DatabaseLibraryException {

		long rowNum = Long.valueOf(rowNumValue);

		String sqlString = "select " + columnNames + " from " + tableName;

		String[] columns = columnNames.split(",");
		String[] values = expectedValues.split("\\|");

		Statement stmt = getConnection().createStatement();
		try {
			stmt.executeQuery(sqlString);
			ResultSet rs = (ResultSet) stmt.getResultSet();

			long count = 0;
			while (rs.next()) {
	
				count++;
				if (count == rowNum) {
	
					for (int i = 0; i < columns.length; i++) {
						String fieldValue = rs.getString(columns[i]);
						System.out.println(columns[i] + " -> " + fieldValue);
	
						if (values[i].equals("(NULL)")) {
							values[i] = "";
						}
	
						if (!fieldValue.equals(values[i])) {
							throw new DatabaseLibraryException("Value found: '"
									+ fieldValue + "'. Expected: '" + values[i]
									+ "'");
						}
					}
					break;
				}
			}
	
			// Rownum does not exist
			if (count != rowNum) {
				throw new DatabaseLibraryException(
						"Given rownum does not exist for statement: " + sqlString);
			}
	
		} finally {
			// stmt.close() automatically takes care of its ResultSet, so no rs.close()
			stmt.close();
		}
	}

	/**
	 * This keyword can be used to check for proper content inside a specific
	 * row in a database table. For this it is possible to give a
	 * comma-separated list of column names in the first parameter and a
	 * pipe-separated list of values in the second parameter. Then the name of
	 * the table and a statement used in the where-clause to identify a concrete
	 * row. The corresponding values are then read from the row identified this
	 * way and compared to the expected values. If all values match the teststep
	 * will pass, otherwise it will fail.
	 * 
	 * If the where-clause will select more or less than exactly one row the
	 * test will fail.
	 * 
	 * Example: 
	 * | Check Content for Row Identified by WhereClause | Name,EMail | John Doe|john.doe@x-files | MySampleTable | Postings=14 |
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void checkContentForRowIdentifiedByWhereClause(String columnNames,
			String expectedValues, String tableName, String whereClause)
			throws SQLException, DatabaseLibraryException {

		String sqlString = "select " + columnNames + " from " + tableName
				+ " where " + whereClause;

		String[] columns = columnNames.split(",");
		String[] values = expectedValues.split("\\|");

		Statement stmt = getConnection().createStatement();
		try {
			stmt.executeQuery(sqlString);
			ResultSet rs = (ResultSet) stmt.getResultSet();
	
			long count = 0;
			while (rs.next()) {
				count++;
				if (count == 1) {
	
					for (int i = 0; i < columns.length; i++) {
						String fieldValue = rs.getString(columns[i]);
						System.out.println(columns[i] + " -> " + fieldValue);
	
						if (values[i].equals("(NULL)")) {
							values[i] = "";
						}
	
						if (!fieldValue.equals(values[i])) {
							throw new DatabaseLibraryException("Value found: '"
									+ fieldValue + "'. Expected: '" + values[i]
									+ "'");
						}
					}
				}
	
				// Throw exception if more than one row is selected by the given
				// "where-clause"
				if (count > 1) {
					throw new DatabaseLibraryException(
							"More than one row fetched by given where-clause for statement: "
									+ sqlString);
				}
			}
	
			// Throw exception if no row was fetched by given where-clause
			if (count == 0) {
				throw new DatabaseLibraryException(
						"No row fetched by given where-clause for statement: "
								+ sqlString);
			}
	
		} finally {
			// stmt.close() automatically takes care of its ResultSet, so no rs.close()
			stmt.close();
		}
	}

	/**
	 * Reads a single value from the given table and column based on the
	 * where-clause passed to the test. If the where-clause identifies more or
	 * less than exactly one row in that table this will result in an error for
	 * this teststep. Otherwise the selected value will be returned.
	 * 
	 * Example: 
	 * | ${VALUE}= | Read single Value from Table | MySampleTable | EMail | Name='John Doe' |
	 * @throws DatabaseLibraryException 
	 * 
	 */
	public String readSingleValueFromTable(String tableName, String columnName,
			String whereClause) throws SQLException, DatabaseLibraryException {

		String ret = "";

		String sql = "select " + columnName + " from " + tableName + " where "
				+ whereClause;
		Statement stmt = getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		try {
			stmt.executeQuery(sql);
			ResultSet rs = (ResultSet) stmt.getResultSet();
			
			if(rs.first()) {
				ret = rs.getString(columnName);
			}
			
			if (rs.next()) {
				throw new DatabaseLibraryException("More than one value fetched for: " + sql);
			}
		} finally {
			// stmt.close() automatically takes care of its ResultSet, so no rs.close()
			stmt.close();
		}

		return ret;
	}

	/**
	 * Can be used to check that the database connection used for executing
	 * tests has the proper transaction isolation level. The string parameter
	 * accepts the following values in a case-insensitive manner:
	 * TRANSACTION_READ_UNCOMMITTED, TRANSACTION_READ_COMMITTED,
	 * TRANSACTION_REPEATABLE_READ, TRANSACTION_SERIALIZABLE or
	 * TRANSACTION_NONE.
	 * 
	 * Example: 
	 * | Transaction Isolation Level Must Be | TRANSACTION_READ_COMMITTED |
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void transactionIsolationLevelMustBe(String levelName)
			throws SQLException, DatabaseLibraryException {

		String transactionName = getTransactionIsolationLevel();

		if (!transactionName.equals(levelName)) {
			throw new DatabaseLibraryException(
					"Expected Transaction Isolation Level: " + levelName
							+ " Level found: " + transactionName);
		}

	}

	/**
	 * Returns a String value that contains the name of the transaction
	 * isolation level of the connection that is used for executing the tests.
	 * Possible return values are: TRANSACTION_READ_UNCOMMITTED,
	 * TRANSACTION_READ_COMMITTED, TRANSACTION_REPEATABLE_READ,
	 * TRANSACTION_SERIALIZABLE or TRANSACTION_NONE.
	 * 
	 * Example: 
	 * | ${TI_LEVEL}= | Get Transaction Isolation Level |
	 * 
	 * @throws SQLException
	 */
	public String getTransactionIsolationLevel() throws SQLException {

		String ret = "";

		int transactionIsolation = getConnection().getTransactionIsolation();

		switch (transactionIsolation) {

		case Connection.TRANSACTION_NONE:
			ret = "TRANSACTION_NONE";
			break;

		case Connection.TRANSACTION_READ_COMMITTED:
			ret = "TRANSACTION_READ_COMMITTED";
			break;

		case Connection.TRANSACTION_READ_UNCOMMITTED:
			ret = "TRANSACTION_READ_UNCOMMITTED";
			break;

		case Connection.TRANSACTION_REPEATABLE_READ:
			ret = "TRANSACTION_REPEATABLE_READ";
			break;

		case Connection.TRANSACTION_SERIALIZABLE:
			ret = "TRANSACTION_SERIALIZABLE";
			break;
		}

		return ret;
	}

	/**
	 * Checks that the primary key columns of a given table match the columns
	 * given as a comma-separated list. Note that the given list must be ordered
	 * by the name of the columns. Upper and lower case for the columns as such
	 * is ignored by comparing the values after converting both to lower case.
	 * 
	 * NOTE: Some database expect the table names to be written all in upper
	 * case letters to be found.
	 * 
	 * Example: 
	 * | Check Primary Key Columns For Table | MySampleTable | Id,Name |
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 * @throws DatabaseLibraryException
	 */
	public void checkPrimaryKeyColumnsForTable(String tableName,
			String columnList) throws SQLException, DatabaseLibraryException {

		String keys = getPrimaryKeyColumnsForTable(tableName);

		columnList = columnList.toLowerCase();
		keys = keys.toLowerCase();

		if (!columnList.equals(keys)) {
			throw new DatabaseLibraryException("Given column list: "
					+ columnList + " Keys found: " + keys);
		}
	}

	/**
	 * Returns a comma-separated list of the primary keys defined for the given
	 * table. The list if ordered by the name of the columns.
	 * 
	 * NOTE: Some database expect the table names to be written all in upper
	 * case letters to be found.
	 * 
	 * Example: 
	 * | ${KEYS}= | Get Primary Key Columns For Table | MySampleTable |
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public String getPrimaryKeyColumnsForTable(String tableName)
			throws SQLException {

		String ret = "";

		DatabaseMetaData dbm = getConnection().getMetaData();
		ResultSet rs = dbm.getPrimaryKeys(null, null, tableName);
		try {
			while (rs.next()) {
				ret = rs.getString("COLUMN_NAME") + ",";
			}
		} finally {
			rs.close();
		}
		
		// Remove the last ","
		if (ret.length() > 0) {
			ret = ret.substring(0, ret.length() - 1);
		}

		return ret;
	}

	/**
	 * Executes the given SQL without any further modifications. The given SQL
	 * must be valid for the database that is used. The main purpose of this
	 * keyword is building some contents in the database used for later testing.
	 * 
	 * NOTE: Use this method with care as you might cause damage to your
	 * database, especially when using this in a productive environment.
	 * 
	 * Example: 
	 * | Execute SQL | CREATE TABLE MyTable (Num INTEGER) |
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void executeSql(String sqlString) throws SQLException {

		Statement stmt = getConnection().createStatement();
		try {
			stmt.execute(sqlString);
		} finally {
			stmt.close();
		}
	}

	/**
	 * Executes the SQL statements contained in the given file without any
	 * further modifications. The given SQL must be valid for the database that
	 * is used. Any lines prefixed with "REM" or "#" are ignored. This keyword
	 * can for example be used to setup database tables from some SQL install
	 * script.
	 * 
	 * Single SQL statements in the file can be spread over multiple lines, but
	 * must be terminated with a semicolon ";". A new statement must always
	 * start in a new line and not in the same line where the previous statement
	 * was terminated by a ";".
	 * 
	 * In case there is a problem in executing any of the SQL statements from
	 * the file the execution is terminated and the operation is rolled back.
	 * 
	 * NOTE: Use this method with care as you might cause damage to your
	 * database, especially when using this in a productive environment.
	 * 
	 * Example: 
	 * | Execute SQL from File | myFile.sql |
	 * 
	 * @throws IOExcetion
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void executeSqlFromFile(String fileName) throws SQLException,
			IOException, DatabaseLibraryException {

		getConnection().setAutoCommit(false);

		FileReader fr = new FileReader(new File(fileName));
		BufferedReader br = new BufferedReader(fr);

		String sql = "";
		String line = "";
		while ((line = br.readLine()) != null) {
			line = line.trim();

			// Ignore lines commented out in the given file
			if (line.toLowerCase().startsWith("rem")) {
				continue;
			}
			if (line.startsWith("#")) {
				continue;
			}

			// Add the line to the current SQL statement
			sql += line;

			// Check if SQL statement is complete, if yes execute
			try {
				if (sql.endsWith(";")) {
					sql = sql.substring(0, sql.length() - 1);
					System.out.println("Executing: " + sql);
					executeSql(sql);
					sql = "";
				}
			} catch (SQLException e) {
				sql = "";
				br.close();
				getConnection().rollback();
				getConnection().setAutoCommit(true);
				throw new DatabaseLibraryException("Error executing: " + sql
						+ " Execution from file rolled back!");
			}
		}

		getConnection().commit();
		getConnection().setAutoCommit(true);
		br.close();
	}

	/**
	 * Executes the SQL statements contained in the given file without any
	 * further modifications. The given SQL must be valid for the database that
	 * is used. Any lines prefixed with "REM" or "#" are ignored. This keyword
	 * can for example be used to setup database tables from some SQL install
	 * script.
	 * 
	 * Single SQL statements in the file can be spread over multiple lines, but
	 * must be terminated with a semicolon ";". A new statement must always
	 * start in a new line and not in the same line where the previous statement
	 * was terminated by a ";".
	 * 
	 * Any errors that might happen during execution of SQL statements are
	 * logged to the Robot Log-file, but otherwise ignored.
	 * 
	 * NOTE: Use this method with care as you might cause damage to your
	 * database, especially when using this in a productive environment.
	 * 
	 * Example: 
	 * | Execute SQL from File | myFile.sql |
	 * 
	 * @throws IOExcetion
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void executeSqlFromFileIgnoreErrors(String fileName)
			throws SQLException, IOException, DatabaseLibraryException {

		getConnection().setAutoCommit(false);

		FileReader fr = new FileReader(new File(fileName));
		BufferedReader br = new BufferedReader(fr);

		String sql = "";
		String line = "";
		while ((line = br.readLine()) != null) {
			line = line.trim();

			// Ignore lines commented out in the given file
			if (line.toLowerCase().startsWith("rem")) {
				continue;
			}
			if (line.startsWith("#")) {
				continue;
			}

			// Add the line to the current SQL statement
			sql += line;

			// Check if SQL statement is complete, if yes execute
			try {
				if (sql.endsWith(";")) {
					sql = sql.substring(0, sql.length() - 1);
					System.out.println("Executing: " + sql + "\n");
					executeSql(sql);
					sql = "";
					System.out.println("\n");
				}
			} catch (SQLException e) {
				System.out.println("Error executing: " + sql + "\n"
						+ e.getMessage() + "\n\n");
				sql = "";
			}
		}

		getConnection().commit();
		getConnection().setAutoCommit(true);
		br.close();
	}

	/**
	 * This keyword checks that a given table contains a given amount of rows
	 * matching a given WHERE clause.
	 * 
	 * For the example this means that the table "MySampleTable" must contain
	 * exactly 2 rows matching the given WHERE, otherwise the teststep will
	 * fail.
	 * 
	 * Example: 
	 * | Verify Number Of Rows Matching Where | MySampleTable | email=x@y.net | 2 |
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void verifyNumberOfRowsMatchingWhere(String tableName, String where,
			String rowNumValue) throws SQLException, DatabaseLibraryException {

		long rowNum = Long.valueOf(rowNumValue);

		long num = getNumberOfRows(tableName, where, (rowNum + 1));
		if (num != rowNum) {
			throw new DatabaseLibraryException("Expecting " + rowNum
					+ " rows, fetched: " + num);
		}
	}

	/**
	 * This keyword can be used to check the inexistence of content inside 
	 * a specific row in a database table defined by a where-clause.
	 * This can be used to validate an exclusion of specific data from a table.
	 *  
	 * Example:
	 * | Row Should Not Exist In Table | MySampleTable | Name='John Doe' |
	 * 
	 * This keyword was introduced in version 1.1.
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void rowShouldNotExistInTable(String tableName, String whereClause) 
		throws SQLException, DatabaseLibraryException {

		String sql = "select * from " + tableName + " where " + whereClause;
		Statement stmt = getConnection().createStatement();
		try {
			stmt.executeQuery(sql);
			ResultSet rs = (ResultSet) stmt.getResultSet();
			if(rs.next() == true) {
				throw new DatabaseLibraryException("Row exists (but should not) for where-clause: " 
						+ whereClause + " in table: " + tableName);
			}
		} finally {
			// stmt.close() automatically takes care of its ResultSet, so no rs.close()
			stmt.close();
		}
	}

	/**
	 * Executes the given SQL without any further modifications and
	 * stores the result in a file.  The SQL query must be valid for
	 * the database that is used. The main purpose of this keyword
	 * is to generate expected result sets for use with keyword
	 * compareQueryResultToFile
	 * 
	 * Example: 
	 * | Store Query Result To File | Select phone, email from addresses where last_name = 'Johnson' | query_result.txt |
	 * 
	 * @throws SQLException
	 * @throws IOException 
	 * @throws DatabaseLibraryException
	 */
	public void storeQueryResultToFile(String sqlString, String fileName) throws SQLException, IOException {

		Statement stmt = getConnection().createStatement();
		try {
			stmt.execute(sqlString);
			ResultSet rs = (ResultSet) stmt.getResultSet();
			ResultSetMetaData rsmd = rs.getMetaData();
		    int numberOfColumns = rsmd.getColumnCount();
		    FileWriter fstream = new FileWriter(fileName);
		    BufferedWriter out = new BufferedWriter(fstream);	    
			while (rs.next()) {
				for (int i = 1; i <= numberOfColumns; i++) {
					rs.getString(i);
					out.write(rs.getString(i) + '|');
				}
				out.write("\n");
			}
			out.close();
		} finally {
			// stmt.close() automatically takes care of its ResultSet, so no rs.close()
			stmt.close();
		}
	}

	/**
	 * Executes the given SQL compares the result to expected
	 * results stored in a file.  Results are stored as strings
	 * separated with pipes ('|') with a pipe following the last 
	 * column.  Rows are separated with a newline.
	 * 
	 * To ensure compares work correctly
	 * The SQL query should
	 * a) specify an order
	 * b) convert non-string fields (especially dates) to a specific format
	 * 
	 * storeQueryResultToFile can be used to generate expected result files
	 * 
	 * Example: 
	 * | Compare Query Result To File | Select phone, email from addresses where last_name = 'Johnson' | query_result.txt |
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 * @throws FileNotFoundException 
	 */
	public void compareQueryResultToFile(String sqlString, String fileName) throws SQLException, DatabaseLibraryException, FileNotFoundException {
	
		Statement stmt = getConnection().createStatement();
	    int numDiffs = 0;
	    int maxDiffs = 10;
	    String diffs = "";
	    try {
			stmt.execute(sqlString);
			ResultSet rs = (ResultSet) stmt.getResultSet();
			ResultSetMetaData rsmd = rs.getMetaData();
		    int numberOfColumns = rsmd.getColumnCount();
		    FileReader fr = new FileReader(fileName); 
		    BufferedReader br = new BufferedReader(fr);
		    String actRow;
		    String expRow;
	
		    int row = 0;
			while (rs.next() && (numDiffs < maxDiffs)) {
				actRow = "";
				row++;
				for (int i = 1; i <= numberOfColumns; i++) {
					actRow += rs.getString(i) + '|';
				}
				expRow = br.readLine();
				if (!actRow.equals(expRow)) {
					numDiffs++;
					diffs += "Row " + row + " does not match:\nexp: " + expRow + "\nact: " +actRow + "\n"; 
				}
			}
			if (br.ready() && numDiffs < maxDiffs) {
				numDiffs++;
				diffs += "More rows in expected file than in query result\n";
			}
			br.close();
		} catch (FileNotFoundException e) {
			throw e;
		} catch (IOException e) {
			numDiffs++;
			diffs += "Fewer rows in expected file than in query result\n";
		} finally {
			// stmt.close() automatically takes care of its ResultSet, so no rs.close()
			stmt.close();
			if (numDiffs > 0) throw new DatabaseLibraryException(diffs);
		}
	}

	

	private void setConnection(Connection connection) {
		this.connection = connection;
	}

	private Connection getConnection() {
		if (connection == null) {
			throw new IllegalStateException("No connection open. Did you forget to run 'Connect To Database' before?");
		}
		return connection;
	}


	private long getNumberOfRows(String tableName) throws SQLException {
		return getNumberOfRows(tableName, Long.MAX_VALUE);
	}

	private long getNumberOfRows(String tableName, long limit) throws SQLException {
		return getNumberOfRows(tableName, null, limit);
	}
	
	/* 
	 * @param limit Limit is used to cut off counting in case count(*) is not supported
	 */
	private long getNumberOfRows(String tableName, String where, long limit)
			throws SQLException {

		// Let's first try with count(*), but this is not supported by all
		// databases.
		// In this case an exception will be thrown and we will read the amount
		// of records the "hard way", but luckily limited by the amount of rows
		// expected,
		// so that this might not be too bad.
		long num = -1;
		try {
			String sql = "select count(*) from " + tableName; 
			if (where != null) {
				sql = sql + " where " + where;
			}
			Statement stmt = getConnection().createStatement();
			try {
				stmt.executeQuery(sql);
				ResultSet rs = (ResultSet) stmt.getResultSet();
				rs.next();
				num = rs.getLong("count(*)");
			} finally {
				// stmt.close() automatically takes care of its ResultSet, so no rs.close()
				stmt.close();
			}
		} catch (SQLException e) {
			String sql = "select * from " + tableName;
			if (where != null) {
				sql = sql + " where " + where;
			}
			Statement stmt = getConnection().createStatement();
			try {
				stmt.executeQuery(sql);
				ResultSet rs = (ResultSet) stmt.getResultSet();
				num = 0;
				while ((rs.next()) && (num < limit)) {
					num++;
				}
			} finally {
				// stmt.close() automatically takes care of its ResultSet, so no rs.close()
				stmt.close();
			}
		}
		return num;
	}
}