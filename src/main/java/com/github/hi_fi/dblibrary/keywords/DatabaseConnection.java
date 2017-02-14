package com.github.hi_fi.dblibrary.keywords;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.robotframework.javalib.annotation.ArgumentNames;
import org.robotframework.javalib.annotation.RobotKeyword;
import org.robotframework.javalib.annotation.RobotKeywords;

@RobotKeywords
public class DatabaseConnection {
	public static final String ROBOT_LIBRARY_SCOPE = "GLOBAL";

	private static String defaultAlias = "default";
	private static String currentConnectionAlias = "";

	private static Map<String, Connection> connectionMap = new HashMap<String, Connection>();

	public DatabaseConnection() {
	}

	@RobotKeyword("Activates the databaseconnection with given alias. \n"
			+ "Please note that connection has to be opened earlier.\n\n" + "Example: \n"
			+ "| Activate Database Connection | ownAlias |")
	@ArgumentNames({ "Database alias=default" })
	public void activateDatabaseConnection(String... aliasParam) {
		String alias = aliasParam.length > 0 ? aliasParam[0] : defaultAlias;
		if (DatabaseConnection.connectionMap.containsKey(alias)) {
			DatabaseConnection.currentConnectionAlias = alias;
		} else {
			throw new IllegalStateException(String.format(
					"No connection open with alias %s. Did you forget to run 'Connect To Database' before?", alias));
		}
	}

	@RobotKeyword("Establish the connection to the database. This is mandatory before any of"
			+ "the other keywords can be used and should be ideally done during the "
			+ "suite setup phase. To avoid problems ensure to close the connection again "
			+ "using the disconnect-keyword.\n\n"
			+ "It must be ensured that the JAR-file containing the given driver can be "
			+ "found from the CLASSPATH when starting robot. Furthermore it must be "
			+ "noted that the connection string is database-specific and must be valid of course.\n\n"
			+ "If alias is given, connection can be later referred with that. If alias was in use, existing connection "
			+ "is replaced with new one\n\n" + "" + "Example: \n"
			+ "| Connect To Database | com.mysql.jdbc.Driver | jdbc:mysql://my.host.name/myinstance | UserName | ThePassword | default |")
	@ArgumentNames({ "Driver class name", "Connection string", "Database username", "Database password",
			"Database alias=default" })
	public void connectToDatabase(String driverClassName, String connectString, String dbUser, String dbPassword,
			String... aliasParam)
			throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		String alias = aliasParam.length > 0 ? aliasParam[0] : defaultAlias;
		Class.forName(driverClassName).newInstance();
		setConnection(DriverManager.getConnection(connectString, dbUser, dbPassword), alias);
	}

	@RobotKeyword("Releases the existing connection to the database. In addition this"
			+ "keyword will log any SQLWarnings that might have been occurred on the connection.\n"
			+ "If current connection is closed and there's still some open, you have to activate that manually.\n"
			+ "Example:\n" + "| Disconnect from Database | default |")
	@ArgumentNames({ "Database alias=default" })
	public void disconnectFromDatabase(String... aliasParam) throws SQLException {
		String alias = aliasParam.length > 0 ? aliasParam[0] : defaultAlias;
		Connection disconnectingConnection = getConnection(alias);

		System.out.println(String.format("SQL Warnings on this connection (%s): %s", alias,
				disconnectingConnection.getWarnings()));
		disconnectingConnection.close();
		DatabaseConnection.connectionMap.remove(alias);
		if (alias.equals(DatabaseConnection.currentConnectionAlias)) {
			DatabaseConnection.currentConnectionAlias = "";
		}
	}

	@RobotKeyword("Deletes the entire content of the given database table. This keyword is"
			+ "useful to start tests in a clean state. Use this keyword with care as"
			+ "accidently execution of this keyword in a productive system will cause"
			+ "heavy loss of data. There will be no rollback possible.\n\n" + "Example: \n"
			+ "| Delete All Rows From Table | MySampleTable |")
	@ArgumentNames({ "Table name" })
	public void deleteAllRowsFromTable(String tableName) throws SQLException {
		String sql = "delete from " + tableName;

		Statement stmt = getConnection().createStatement();
		try {
			stmt.execute(sql);
		} finally {
			stmt.close();
		}
	}

	@RobotKeyword("This keyword can be used to check for proper content inside a specific "
			+ "row in a database table. For this it is possible to give a "
			+ "comma-separated list of column names in the first parameter and a "
			+ "pipe-separated list of values in the second parameter. Then the name of "
			+ "the table and the rownum to check must be passed to this keyword. The "
			+ "corresponding values are then read from that row in the given table and "
			+ "compared to the expected values. If all values match the teststep will "
			+ "pass, otherwise it will fail. " + "\n\n" + "Example: \n"
			+ "| Check Content for Row Identified by Rownum | Name,EMail | John Doe|john.doe@x-files | MySampleTable | 4 | ")
	@ArgumentNames({ "Column names (comma separated)", "Expected values (pipe separated)", "Table name",
			"Number of row to check" })
	public void checkContentForRowIdentifiedByRownum(String columnNames, String expectedValues, String tableName,
			String rowNumValue) throws SQLException, DatabaseLibraryException {

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
							throw new DatabaseLibraryException(
									"Value found: '" + fieldValue + "'. Expected: '" + values[i] + "'");
						}
					}
					break;
				}
			}

			// Rownum does not exist
			if (count != rowNum) {
				throw new DatabaseLibraryException("Given rownum does not exist for statement: " + sqlString);
			}

		} finally {
			// stmt.close() automatically takes care of its ResultSet, so no
			// rs.close()
			stmt.close();
		}
	}

	@RobotKeyword("This keyword can be used to check for proper content inside a specific "
			+ "row in a database table. For this it is possible to give a "
			+ "comma-separated list of column names in the first parameter and a "
			+ "pipe-separated list of values in the second parameter. Then the name of "
			+ "the table and a statement used in the where-clause to identify a concrete "
			+ "row. The corresponding values are then read from the row identified this "
			+ "way and compared to the expected values. If all values match the teststep "
			+ "will pass, otherwise it will fail. " + "\n\n"
			+ "If the where-clause will select more or less than exactly one row the " + "test will fail. " + "\n\n"
			+ "Example: \n"
			+ "| Check Content for Row Identified by WhereClause | Name,EMail | John Doe|john.doe@x-files | MySampleTable | Postings=14 | ")
	@ArgumentNames({ "Column names (comma separated)", "Expected values (pipe separated)", "Table name",
			"Where clause to identify the row" })
	public void checkContentForRowIdentifiedByWhereClause(String columnNames, String expectedValues, String tableName,
			String whereClause) throws SQLException, DatabaseLibraryException {

		String sqlString = "select " + columnNames + " from " + tableName + " where " + whereClause;

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
							throw new DatabaseLibraryException(
									"Value found: '" + fieldValue + "'. Expected: '" + values[i] + "'");
						}
					}
				}

				// Throw exception if more than one row is selected by the given
				// "where-clause"
				if (count > 1) {
					throw new DatabaseLibraryException(
							"More than one row fetched by given where-clause for statement: " + sqlString);
				}
			}

			// Throw exception if no row was fetched by given where-clause
			if (count == 0) {
				throw new DatabaseLibraryException("No row fetched by given where-clause for statement: " + sqlString);
			}

		} finally {
			// stmt.close() automatically takes care of its ResultSet, so no
			// rs.close()
			stmt.close();
		}
	}

	@RobotKeyword("Reads a single value from the given table and column based on the "
			+ "where-clause passed to the test. If the where-clause identifies more or "
			+ "less than exactly one row in that table this will result in an error for "
			+ "this teststep. Otherwise the selected value will be returned. " + "\n\n" + "Example: \n"
			+ "| ${VALUE}= | Read single Value from Table | MySampleTable | EMail | Name='John Doe' | ")
	@ArgumentNames({ "Table name", "Column to get", "Where clause to identify the row" })
	public String readSingleValueFromTable(String tableName, String columnName, String whereClause)
			throws SQLException, DatabaseLibraryException {

		String ret = "";

		String sql = "select " + columnName + " from " + tableName + " where " + whereClause;
		Statement stmt = getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		try {
			stmt.executeQuery(sql);
			ResultSet rs = (ResultSet) stmt.getResultSet();

			if (rs.first()) {
				ret = rs.getString(columnName);
			}

			if (rs.next()) {
				throw new DatabaseLibraryException("More than one value fetched for: " + sql);
			}
		} finally {
			// stmt.close() automatically takes care of its ResultSet, so no
			// rs.close()
			stmt.close();
		}

		return ret;
	}

	@RobotKeyword("Can be used to check that the database connection used for executing "
			+ "tests has the proper transaction isolation level. The string parameter "
			+ "accepts the following values in a case-insensitive manner: "
			+ "TRANSACTION_READ_UNCOMMITTED, TRANSACTION_READ_COMMITTED, "
			+ "TRANSACTION_REPEATABLE_READ, TRANSACTION_SERIALIZABLE or " + "TRANSACTION_NONE. " + "\n\n"
			+ "Example: \n" + "| Transaction Isolation Level Must Be | TRANSACTION_READ_COMMITTED | ")
	@ArgumentNames({ "Isolation level" })
	public void transactionIsolationLevelMustBe(String levelName) throws SQLException, DatabaseLibraryException {

		String transactionName = getTransactionIsolationLevel();

		if (!transactionName.equals(levelName)) {
			throw new DatabaseLibraryException(
					"Expected Transaction Isolation Level: " + levelName + " Level found: " + transactionName);
		}

	}

	@RobotKeyword("Returns a String value that contains the name of the transaction "
			+ "isolation level of the connection that is used for executing the tests. "
			+ "Possible return values are: TRANSACTION_READ_UNCOMMITTED, "
			+ "TRANSACTION_READ_COMMITTED, TRANSACTION_REPEATABLE_READ, "
			+ "TRANSACTION_SERIALIZABLE or TRANSACTION_NONE. " + "\n\n" + "Example: \n"
			+ "| ${TI_LEVEL}= | Get Transaction Isolation Level | ")
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

	@RobotKeyword("Checks that the primary key columns of a given table match the columns "
			+ "given as a comma-separated list. Note that the given list must be ordered "
			+ "by the name of the columns. Upper and lower case for the columns as such "
			+ "is ignored by comparing the values after converting both to lower case. " + "\n\n"
			+ "*NOTE*: Some database expect the table names to be written all in upper " + "case letters to be found. "
			+ "\n\n" + "Example: \n" + "| Check Primary Key Columns For Table | MySampleTable | Id,Name |")
	@ArgumentNames({ "Table name", "Comma separated list of primary key columns to check" })
	public void checkPrimaryKeyColumnsForTable(String tableName, String columnList)
			throws SQLException, DatabaseLibraryException {

		String keys = getPrimaryKeyColumnsForTable(tableName);

		columnList = columnList.toLowerCase();
		keys = keys.toLowerCase();

		if (!columnList.equals(keys)) {
			throw new DatabaseLibraryException("Given column list: " + columnList + " Keys found: " + keys);
		}
	}

	@RobotKeyword("Returns a comma-separated list of the primary keys defined for the given "
			+ "table. The list if ordered by the name of the columns. " + "\n\n"
			+ "*NOTE*: Some database expect the table names to be written all in upper " + "case letters to be found. "
			+ "\n\n" + "Example: \n" + "| ${KEYS}= | Get Primary Key Columns For Table | MySampleTable | ")
	@ArgumentNames({ "Table name" })
	public String getPrimaryKeyColumnsForTable(String tableName) throws SQLException {

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

	@RobotKeyword("This keyword can be used to check the inexistence of content inside a "
			+ "specific row in a database table defined by a where-clause. This can be "
			+ "used to validate an exclusion of specific data from a table. " + "\n\n" + "Example: \n"
			+ "| Row Should Not Exist In Table | MySampleTable | Name='John Doe' | " + "\n\n"
			+ "This keyword was introduced in version 1.1. ")
	@ArgumentNames({ "Table to check", "Where clause" })
	public void rowShouldNotExistInTable(String tableName, String whereClause)
			throws SQLException, DatabaseLibraryException {

		String sql = "select * from " + tableName + " where " + whereClause;
		Statement stmt = getConnection().createStatement();
		try {
			stmt.executeQuery(sql);
			ResultSet rs = (ResultSet) stmt.getResultSet();
			if (rs.next() == true) {
				throw new DatabaseLibraryException(
						"Row exists (but should not) for where-clause: " + whereClause + " in table: " + tableName);
			}
		} finally {
			// stmt.close() automatically takes care of its ResultSet, so no
			// rs.close()
			stmt.close();
		}
	}

	@RobotKeyword("Executes the given SQL without any further modifications and stores the "
			+ "result in a file. The SQL query must be valid for the database that is "
			+ "used. The main purpose of this keyword is to generate expected result "
			+ "sets for use with keyword compareQueryResultToFile " + "\n\n"
			+ "*NOTE*: If using keyword remotely, file need to be trasfered to server some "
			+ "other way; this library is not doing the transfer." + "\n\n" + "Example: \n"
			+ "| Store Query Result To File | Select phone, email from addresses where last_name = 'Johnson' | query_result.txt | ")
	@ArgumentNames({ "Query to execute", "File to save results" })
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
			// stmt.close() automatically takes care of its ResultSet, so no
			// rs.close()
			stmt.close();
		}
	}

	private void setConnection(Connection connection, String alias) {
		DatabaseConnection.connectionMap.put(alias, connection);
		DatabaseConnection.currentConnectionAlias = alias;
	}

	public static Connection getConnection() {
		return getConnection(currentConnectionAlias);
	}

	public static Connection getConnection(String alias) {
		if (!DatabaseConnection.connectionMap.containsKey(alias)) {
			throw new IllegalStateException(String.format(
					"No connection open with alias %s. Did you forget to run 'Connect To Database' before?", alias));
		}
		return DatabaseConnection.connectionMap.get(alias);
	}
}