package org.robot.database.keywords;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
 * <pre>
 * The following table lists some examples of drivers and connection strings
 * for some popular databases. 
 * | Database | Driver Name | Sample Connection String | Download Driver |
 * | MySql | com.mysql.jdbc.Driver | jdbc:mysql://servername/dbname | http://dev.mysql.com/downloads/connector/j/ |
 * | Oracle | oracle.jdbc.driver.OracleDriver | jdbc:oracle:thin:@servername:port:dbname | http://www.oracle.com/technology/tech/java/sqlj_jdbc/htdocs/jdbc_faq.html |
 * </pre>
 * 
 * The examples in the description of the keywords is based on a database table
 * named "MySampleTable" that has the following layout:
 * 
 * <pre>
 * MySampleTable: 
 * | COLUMN | TYPE | 
 * | Id | Number | 
 * | Name | String | 
 * | EMail | String | 
 * | Postings | Number | 
 * | State | Number | 
 * | LastPosting | Timestamp |
 * </pre>
 * 
 * NOTE: A lot of keywords that are targeted for Tables will work equally with
 * Views as this is often no difference if Select-statements are performed.
 * 
 */
public class DatabaseLibrary {
	public static final String ROBOT_LIBRARY_SCOPE = "GLOBAL";

	private Connection connection = null;

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
	 * <pre>
	 * Example: 
	 * | Connect To Database | com.mysql.jdbc.Driver | jdbc:mysql://my.host.name/myinstance | UserName | ThePassword |
	 * </pre>
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
	 * <pre>
	 * Example: 
	 * | Disconnect from Database |
	 * </pre>
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
	 * <pre>
	 * Example: 
	 * | Table Must Exist | MySampleTable |
	 * </pre>
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void tableMustExist(String tableName) throws SQLException,
			DatabaseLibraryException {

		DatabaseMetaData dbm = getConnection().getMetaData();
		ResultSet rs = dbm.getTables(null, null, tableName, null);
		if (!rs.next()) {
			throw new DatabaseLibraryException("Table: " + tableName
					+ " was not found");
		}
	}

	/**
	 * Checks that the given table has no rows. It is a convenience way of using
	 * the "Table Must Contain Number Of Rows" with zero for the amount of rows.
	 * 
	 * <pre>
	 * Example: 
	 * | Table Must Be Empty | MySampleTable |
	 * </pre>
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
	 * <pre>
	 * Example: 
	 * | Delete All Rows From Table | MySampleTable |
	 * </pre>
	 * 
	 * @throws SQLException
	 */
	public void deleteAllRowsFromTable(String tableName) throws SQLException {
		String sql = "delete from " + tableName;

		Statement stmt = getConnection().createStatement();
		stmt.executeQuery(sql);
		stmt.close();
	}

	/**
	 * This keyword checks that a given table contains a given amount of rows.
	 * For the example this means that the table "MySampleTable" must contain
	 * exactly 14 rows, otherwise the teststep will fail.
	 * 
	 * <pre>
	 * Example: 
	 * | Table Must Contain Number Of Rows | MySampleTable | 14 |
	 * </pre>
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void tableMustContainNumberOfRows(String tableName, String rowNumValue)
			throws SQLException, DatabaseLibraryException {

		long rowNum = new Long(rowNumValue).longValue();
		
		long num = getNumberOfRows(tableName, (rowNum + 1));
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
	 * <pre>
	 * Example: 
	 * | Table Must Contain More Than Number Of Rows | MySampleTable | 99 |
	 * </pre>
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void tableMustContainMoreThanNumberOfRows(String tableName,
			String rowNumValue) throws SQLException, DatabaseLibraryException {

		long rowNum = new Long(rowNumValue).longValue();
		
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
	 * <pre>
	 * Example: 
	 * | Table Must Contain Less Than Number Of Rows | MySampleTable | 1001 |
	 * </pre>
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void tableMustContainLessThanNumberOfRows(String tableName,
			String rowNumValue) throws SQLException, DatabaseLibraryException {

		long rowNum = new Long(rowNumValue).longValue();
		
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
	 * <pre>
	 * Example: 
	 * | Tables Must Contain Same Amount Of Rows | MySampleTable | MyCompareTable |
	 * </pre>
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
	 * comma-separated list of values in the second parameter. Then the name of
	 * the table and the rownum to check must be passed to this keyword. The
	 * corresponding values are then read from that row in the given table and
	 * compared to the expected values. If all values match the teststep will
	 * pass, otherwise it will fail.
	 * 
	 * <pre>
	 * Example: 
	 * | Check Content for Row Identified by Rownum | Name, EMail | John Doe, john.doe@x-files | MySampleTable | 4 |
	 * </pre>
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void checkContentForRowIdentifiedByRownum(String columnNames,
			String expectedValues, String tableName, String rowNumValue)
			throws SQLException, DatabaseLibraryException {
		
		long rowNum = new Long(rowNumValue).longValue();

		String sqlString = "select " + columnNames + " from " + tableName;

		String[] columns = columnNames.split(",");
		String[] values = expectedValues.split("\\|");

		Statement stmt = getConnection().createStatement();
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

		rs.close();
		stmt.close();
	}

	/**
	 * This keyword can be used to check for proper content inside a specific
	 * row in a database table. For this it is possible to give a
	 * comma-separated list of column names in the first parameter and a
	 * comma-separated list of values in the second parameter. Then the name of
	 * the table and a statement used in the where-clause to identify a concrete
	 * row. The corresponding values are then read from the row identified this
	 * way and compared to the expected values. If all values match the teststep
	 * will pass, otherwise it will fail.
	 * 
	 * If the where-clause will select more or less than exactly one row the
	 * test will fail.
	 * 
	 * <pre>
	 * Example: 
	 * | Check Content for Row Identified by WhereClause | Name,EMail | John Doe,john.doe@x-files | MySampleTable | Postings=14 |
	 * </pre>
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

		// Throw exception if no rrow was fetched by given where-clause
		if (count == 0) {
			throw new DatabaseLibraryException(
					"No row fetched by given where-clause for statement: "
							+ sqlString);
		}

		rs.close();
		stmt.close();
	}

	/**
	 * Reads a single value from the given table and column based on the
	 * where-clause passed to the test. If the where-clause identifies more or
	 * less than exactly one row in that table this will result in an error for
	 * this teststep. Otherwise the selected value will be returned.
	 * 
	 * <pre>
	 * Example: 
	 * | ${VALUE}= | Read single Value from Table | MySampleTable | EMail | Name='John Doe' |
	 * </pre>
	 * 
	 */
	public String readSingleValueFromTable(String tableName, String columnName,
			String whereClause) throws SQLException {

		String ret = "";

		String sql = "select " + columnName + " from " + tableName + " where "
				+ whereClause;
		Statement stmt = getConnection().createStatement();
		stmt.executeQuery(sql);
		ResultSet rs = (ResultSet) stmt.getResultSet();
		rs.next();
		ret = rs.getString(columnName);
		rs.close();
		stmt.close();

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
	 * <pre>
	 * Example: 
	 * | Transaction Isolation Level Must Be | TRANSACTION_READ_COMMITTED |
	 * </pre>
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void transactionIsolationLevelMustBe(String levelName)
			throws SQLException, DatabaseLibraryException {

		String transactionName = "";

		int transactionIsolation = getConnection().getTransactionIsolation();

		switch (transactionIsolation) {

		case Connection.TRANSACTION_NONE:
			transactionName = "TRANSACTION_NONE";
			break;

		case Connection.TRANSACTION_READ_COMMITTED:
			transactionName = "TRANSACTION_READ_COMMITTED";
			break;

		case Connection.TRANSACTION_READ_UNCOMMITTED:
			transactionName = "TRANSACTION_READ_UNCOMMITTED";
			break;

		case Connection.TRANSACTION_REPEATABLE_READ:
			transactionName = "TRANSACTION_REPEATABLE_READ";
			break;

		case Connection.TRANSACTION_SERIALIZABLE:
			transactionName = "TRANSACTION_SERIALIZABLE";
			break;
		}

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
	 * <pre>
	 * Example: 
	 * | ${TI_LEVEL}= | Get Transaction Isolation Level |
	 * </pre>
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
	 * <pre>
	 * Example: 
	 * | Check Primary Key Columns For Table | MySampleTable | Id,Name |
	 * </pre>
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException 
	 * @throws DatabaseLibraryException
	 */
	public void checkPrimaryKeyColumnsForTable(String tableName,
			String columnList) throws SQLException, DatabaseLibraryException {

		String keys = "";

		DatabaseMetaData dbm = getConnection().getMetaData();
		ResultSet rs = dbm.getPrimaryKeys(null, null, tableName);

		while (rs.next()) {
			keys = rs.getString("COLUMN_NAME") + ",";
		}

		// Remove the last ","
		if (keys.length() > 0) {
			keys = keys.substring(0, keys.length() - 1);
		}

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
	 * <pre>
	 * Example: 
	 * | ${KEYS}= | Get Primary Key Columns For Table | MySampleTable |
	 * </pre>
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public String getPrimaryKeyColumnsForTable(String tableName)
			throws SQLException {

		String ret = "";

		DatabaseMetaData dbm = getConnection().getMetaData();
		ResultSet rs = dbm.getPrimaryKeys(null, null, tableName);

		while (rs.next()) {
			ret = rs.getString("COLUMN_NAME") + ",";
		}

		// Remove the last ","
		if (ret.length() > 0) {
			ret = ret.substring(0, ret.length() - 1);
		}

		return ret;
	}
	
	/**
	 * Executes the given SQL without any further modifications.O
	 * The given SQL must be valid for the database that is used.
	 * The main purpose of this keyword is building some contents in the database
	 * used for later testing.
	 * 
	 * NOTE: Use this method with care as you might cause damage to your database,
	 * especially when using this in a productive environment.
	 * 
	 * <pre>
	 * Example: 
	 * | Execute SQL | CREATE TABLE MyTable (Num INTEGER) |
	 * </pre>
	 * 
	 * @throws SQLException
	 * @throws DatabaseLibraryException
	 */
	public void executeSQL(String sqlString)
			throws SQLException {

		Statement stmt = getConnection().createStatement();
		stmt.execute(sqlString);
	}
	
	
	

	private void setConnection(Connection connection) {
		this.connection = connection;
	}

	private Connection getConnection() {
		return connection;
	}

	private long getNumberOfRows(String tableName, long limit)
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
			Statement stmt = getConnection().createStatement();
			stmt.executeQuery(sql);
			ResultSet rs = (ResultSet) stmt.getResultSet();
			rs.next();
			num = rs.getLong("count(*)");
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			String sql = "select * from " + tableName;
			Statement stmt = getConnection().createStatement();
			stmt.executeQuery(sql);
			ResultSet rs = (ResultSet) stmt.getResultSet();
			num = 0;
			while ((rs.next()) && (num < limit)) {
				num++;
			}
			rs.close();
			stmt.close();
		}
		return num;
	}

	private long getNumberOfRows(String tableName) throws SQLException {

		// Let's first try with count(*), but this is not supported by all
		// databases.
		// In this case an exception will be thrown and we will read the amount
		// of records the "hard way"
		long num = -1;
		try {
			String sql = "select count(*) from " + tableName;
			Statement stmt = getConnection().createStatement();
			stmt.executeQuery(sql);
			ResultSet rs = (ResultSet) stmt.getResultSet();
			rs.next();
			num = rs.getLong("count(*)");
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			String sql = "select * from " + tableName;
			Statement stmt = getConnection().createStatement();
			stmt.executeQuery(sql);
			ResultSet rs = (ResultSet) stmt.getResultSet();
			num = 0;
			while (rs.next()) {
				num++;
			}
			rs.close();
			stmt.close();
		}
		return num;
	}
}
