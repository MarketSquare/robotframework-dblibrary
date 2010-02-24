package org.robot.database.keywords;

import java.sql.Connection;
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
 * TESTED DATABASES AND SAMPLE CONNECTION STRINGS TO BE ADDED HERE.
 * 
 * The examples in the description of the keywords is based on a database table
 * named "MySampleTable" that has the following layout:
 * 
 * MySampleTable: | COLUMN | TYPE | | Id | Number | | Name | String | | EMail |
 * String | | Postings | Number | | State | Number | | LastPosting | Timestamp |
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
	 * found from the CLASSPATH when starting robot. Furthremore it must be
	 * noted that the connection string is database-specific and must be valid
	 * of course.
	 * 
	 * Example: | Connect To Database | com.mysql.jdbc.Driver |
	 * jdbc:mysql://my.host.name/myinstance | UserName | ThePassword |
	 * 
	 */
	public void connectToDatabase(String driverClassName, String connectString,
			String dbUser, String dbPassword) throws Exception {

		Class.forName(driverClassName).newInstance();
		setConnection(DriverManager.getConnection(connectString, dbUser,
				dbPassword));
	}

	/**
	 * Releases the existing connection to the database. In addition this
	 * keyword will log any SQLWarnings that might have been occurred on the
	 * connection.
	 * 
	 * Example: | Disconnect from Database |
	 * 
	 */
	public void disconnectFromDatabase() throws SQLException {
		getConnection().close();
	}

	/**
	 * Checks that a table with the given name exists. If the table does not
	 * exist the test will fail.
	 * 
	 * Example: | Table Must Exist | MySampleTable |
	 * 
	 */
	public void tableMustExist(String tableName) {

	}

	/**
	 * Checks that the given table has no rows. It is a convinience way of using
	 * the "Table Must Contain Number OF Rows" with zero for the amount of rows.
	 * 
	 * Example: | Table Must Be Empty | MySampleTable |
	 * 
	 * @throws Exception
	 * 
	 */
	public void tableMustBeEmpty(String tableName) throws Exception {
		tableMustContainNumberOfRows(tableName, 0);
	}

	/**
	 * Deletes the entire content of the given database table. This keyword is
	 * useful to start tests in a clean state. Use this keyword with care as
	 * accidently execution of this keyword in a productive system will cause
	 * heavy loss of data. There will be no rollback possible.
	 * 
	 * Example: | Delete All Rows From Table | MySampleTable |
	 * 
	 * @throws SQLException
	 * 
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
	 * Example: | Table Must Contain Number Of Rows | MySampleTable | 14 |
	 * 
	 * @throws Exception
	 */
	public void tableMustContainNumberOfRows(String tableName, long rowNum)
			throws Exception {

		long num = getNumberOfRows(tableName);
		if (num != rowNum) {
			throw new Exception("Expecting " + rowNum + " rows, fetched: "
					+ num);
		}
	}

	/**
	 * This keyword checks that a given table contains more than the given
	 * amount of rows. For the example this means that the table "MySampleTable"
	 * must contain 100 or more rows, otherwise the teststep will fail.
	 * 
	 * Example: | Table Must Contain More Than Number Of Rows | MySampleTable |
	 * 99 |
	 */
	public void tableMustContainMoreThanNumberOfRows(String tableName,
			long rowNum) throws Exception {

		long num = getNumberOfRows(tableName);
		if (num <= rowNum) {
			throw new Exception("Expecting more than" + rowNum
					+ " rows, fetched: " + num);
		}
	}

	/**
	 * This keyword checks that a given table contains less than the given
	 * amount of rows. For the example this means that the table "MySampleTable"
	 * must contain anything between 0 and 1000 rows, otherwise the teststep
	 * will fail.
	 * 
	 * Example: | Table Must Contain Less Than Number Of Rows | MySampleTable |
	 * 1001 |
	 */
	public void tableMustContainLessThanNumberOfRows(String tableName,
			long rowNum) throws Exception {

		long num = getNumberOfRows(tableName);
		if (num >= rowNum) {
			throw new Exception("Expecting less than" + rowNum
					+ " rows, fetched: " + num);
		}
	}

	/**
	 * This keyword checks that two given database tables have the same amount
	 * of rows.
	 * 
	 * Example: | Tables Must Contain Same Amount Of Rows | MySampleTable |
	 * MyCompareTable |
	 */
	public void tablesMustContainSameAmountOfRows(String firstTableName,
			String secondTableName) throws Exception {

		long firstNum = getNumberOfRows(firstTableName);
		long secondNum = getNumberOfRows(secondTableName);

		if (firstNum != secondNum) {
			throw new Exception("Expecting same amount of rows, but table "
					+ firstTableName + " has " + firstNum + " rows and table "
					+ secondTableName + " has " + secondNum + " rows!");
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
	 * Example: | Check Content for Row Identified by Rownum | Name, EMail |
	 * John Doe, john.doe@x-files | MySampleTable | 4 |
	 * 
	 */
	public void checkContentForRowIdentifiedByRownum(String columnNames,
			String expectedValues, String tableName, String rowNum)
			throws Exception {
		
		String sqlString = "select " + columnNames + " from " + tableName;

		String[] columns = columnNames.split(",");
		String[] values = expectedValues.split("\\|");

		Statement stmt = getConnection().createStatement();
		stmt.executeQuery(sqlString);
		ResultSet rs = (ResultSet) stmt.getResultSet();

		int count = 0;
		while (rs.next()) {

			count++;
			if (count == new Integer(rowNum).intValue()) {

				for (int i = 0; i < columns.length; i++) {
					String fieldValue = rs.getString(columns[i]);
					System.out.println(columns[i] + " -> " + fieldValue);

					if (values[i].equals("(NULL)")) {
						values[i] = "";
					}

					if (!fieldValue.equals(values[i])) {
						throw new Exception("Value found: '" + fieldValue + "'. Expected: '" + values[i] + "'");
					}
				}
			}
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
	 * Example: | Check Content for Row Identified by Rownum | Name,EMail | John
	 * Doe,john.doe@x-files | MySampleTable | Postings=14 |
	 * 
	 */
	public void checkContentForRowIdentifiedByWhereClause(String spaltennamen,
			String erwarteteWerte, String tabellenname, String whereClause)
			throws Exception {
	}

	/**
	 * Updates the given table to change the values of a given column to a new
	 * Value for those rows selected by the given where-clause.
	 * 
	 * Example: | Update Column Values in Table | MySampleTable | State | 0 |
	 * LastPosting < sysdate-100 |
	 * 
	 */
	public void updateColumnValuesInTable(String tableName, String columnName,
			String newValue, String whereClause) throws SQLException {
	}

	/**
	 * Reads a single value from the given table and column based on the
	 * where-clause passed to the test. If the where-clause identifies more or
	 * less than exactly one row in that table this will result in an error for
	 * this teststep. Otherwise the selected value will be returned.
	 * 
	 * Example: | ${VALUE}= | Read single Value from Table | MySampleTable |
	 * EMail | Name='John Doe' |
	 * 
	 */
	public String readSingleValueFromTable(String tableName, String columnName,
			String whereClause) throws Exception {
		return "";
	}

	/**
	 * Exports the data from the given table into a file that is stored at the
	 * given location. The where-clause can (and should) be used to narrow the
	 * amount of rows that is exported this way. The file is stored in some
	 * simple XML-format and can be imported again to the database using the
	 * "Import Data From File" keyword. This way it is possible to store a set
	 * of testdata permanently retrieving it for example from some Live- or
	 * Demosystem. This keyword will probably have some issues if millions of
	 * rows are exported/imported using it.
	 * 
	 * The keyword returns the amount of rows written to the XML-file.
	 * 
	 * Example: | ${ROWSEXPORTED}= | MySampleTable | /tmp/mysampletable.xml |
	 * Timestamp > sysdate-50 |
	 * 
	 */
	public int exportDataFromTable(String tableName, String filePath,
			String whereClause) {
		return 0;
	}

	/**
	 * This keyword reads data from a XML-file and stores the corresponding data
	 * to the database. The file must have been created using the
	 * "Export Data From Table" keyword or it must be created manually in the
	 * exact format. The XML-file contains not only the data as such, but also
	 * the name of the schema and table from which the data was exported. The
	 * same information is used for the import.
	 * 
	 * The keyword returns the amount of rows that have been successfully stored
	 * to the database table.
	 * 
	 * Example: | ${ROWSIMPORTED}= | /tmp/mysampletable.xml |
	 * 
	 */
	public int importDataFromFile(String filePath) {
		return 0;
	}

	/**
	 * Can be used to check that the database connection used for executing
	 * tests has the proper transaction isolation level. The string parameter
	 * accepts the following values in a case-insensitive manner:
	 * TRANSACTION_READ_UNCOMMITTED, TRANSACTION_READ_COMMITTED,
	 * TRANSACTION_REPEATABLE_READ, TRANSACTION_SERIALIZABLE or
	 * TRANSACTION_NONE.
	 * 
	 * Example: | Transaction Isolation Level Must Be |
	 * TRANSACTION_READ_COMMITTED |
	 * 
	 */
	public void transactionIsolationLevelMustBe(String levelName) {
	}

	/**
	 * Returns a String value that contains the name of the transaction
	 * isolation level of the connection that is used for executing the tests.
	 * Possible return vlaues are: TRANSACTION_READ_UNCOMMITTED,
	 * TRANSACTION_READ_COMMITTED, TRANSACTION_REPEATABLE_READ,
	 * TRANSACTION_SERIALIZABLE or TRANSACTION_NONE.
	 * 
	 * Example: | ${TI_LEVEL}= | Get Transaction Isolation Level |
	 * 
	 */
	public String getTransactionIsolationLevel() {
		return "";
	}

	private void setConnection(Connection connection) {
		this.connection = connection;
	}

	public Connection getConnection() {
		return connection;
	}

	private long getNumberOfRows(String tableName) throws SQLException {
		String sql = "select count(*) from " + tableName;
		Statement stmt = getConnection().createStatement();
		stmt.executeQuery(sql);
		ResultSet rs = (ResultSet) stmt.getResultSet();
		rs.next();
		long num = rs.getLong("count(*)");
		rs.close();
		stmt.close();

		return num;
	}
}
