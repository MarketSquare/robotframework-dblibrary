package com.github.hi_fi.dblibrary.keywords;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.robotframework.javalib.annotation.ArgumentNames;
import org.robotframework.javalib.annotation.RobotKeyword;
import org.robotframework.javalib.annotation.RobotKeywords;

@RobotKeywords
public class Assert {
	
	@RobotKeyword("Checks that the given table has no rows. It is a convenience way of using "
			+ "the 'Table Must Contain Number Of Rows' with zero for the amount of rows.\n\n" + "Example: \n"
			+ "| Table Must Be Empty | MySampleTable |")
	@ArgumentNames({ "Table name" })
	public void tableMustBeEmpty(String tableName) throws SQLException, DatabaseLibraryException {
		tableMustContainNumberOfRows(tableName, "0");
	}
	
	@RobotKeyword("This keyword checks that a given table contains less than the given "
			+ "amount of rows. For the example this means that the table \"MySampleTable\"\n\n"
			+ "must contain anything between 0 and 1000 rows, otherwise the teststep " + "will fail. " + "\n\n"
			+ "Example: \n" + "| Table Must Contain Less Than Number Of Rows | MySampleTable | 1001 | ")
	@ArgumentNames({ "Table name", "Number of rows too high" })
	public void tableMustContainLessThanNumberOfRows(String tableName, String rowNumValue)
			throws SQLException, DatabaseLibraryException {

		long rowNum = Long.valueOf(rowNumValue);

		long num = getNumberOfRows(tableName, rowNum);
		if (num >= rowNum) {
			throw new DatabaseLibraryException("Expecting less than" + rowNum + " rows, fetched: " + num);
		}
	}
	
	@RobotKeyword("This keyword checks that a given table contains more than the given "
			+ "amount of rows. For the example this means that the table \"MySampleTable\""
			+ "must contain 100 or more rows, otherwise the teststep will fail. " + "\n\n" + "Example: \n"
			+ "| Table Must Contain More Than Number Of Rows | MySampleTable | 99 | ")
	@ArgumentNames({ "Table name", "Number of rows too low" })
	public void tableMustContainMoreThanNumberOfRows(String tableName, String rowNumValue)
			throws SQLException, DatabaseLibraryException {

		long rowNum = Long.valueOf(rowNumValue);

		long num = getNumberOfRows(tableName, rowNum + 1);
		if (num <= rowNum) {
			throw new DatabaseLibraryException("Expecting more than" + rowNum + " rows, fetched: " + num);
		}
	}
	
	@RobotKeyword("This keyword checks that a given table contains a given amount of rows. "
			+ "For the example this means that the table \"MySampleTable\" must contain "
			+ " exactly 14 rows, otherwise the teststep will fail.\n\n" + " Example: \n"
			+ "| Table Must Contain Number Of Rows | MySampleTable | 14 |")
	@ArgumentNames({ "Table name", "Amount of rows expected" })
	public void tableMustContainNumberOfRows(String tableName, String rowNumValue)
			throws SQLException, DatabaseLibraryException {

		long rowNum = Long.valueOf(rowNumValue);

		long num = getNumberOfRows(tableName, rowNum + 1);
		if (num != rowNum) {
			throw new DatabaseLibraryException("Expecting " + rowNum + " rows, fetched: " + num);
		}
	}

	@RobotKeyword("This keyword checks that two given database tables have the same amount " + "of rows. " + "\n\n"
			+ "Example: \n" + "| Tables Must Contain Same Amount Of Rows | MySampleTable | MyCompareTable | ")
	@ArgumentNames({ "First table name", "Second table name" })
	public void tablesMustContainSameAmountOfRows(String firstTableName, String secondTableName)
			throws SQLException, DatabaseLibraryException {

		long firstNum = getNumberOfRows(firstTableName);
		long secondNum = getNumberOfRows(secondTableName);

		if (firstNum != secondNum) {
			throw new DatabaseLibraryException("Expecting same amount of rows, but table " + firstTableName + " has "
					+ firstNum + " rows and table " + secondTableName + " has " + secondNum + " rows!");
		}
	}
	
	@RobotKeyword("Checks that a table with the given name exists. If the table does not exist the test will fail.\n\n"
			+ "*NOTE*: Some database expect the table names to be written all in upper case letters to be found.\n\n"
			+ "Example: \n " + "| Table Must Exist | MySampleTable |")
	@ArgumentNames({ "Table name" })
	public void tableMustExist(String tableName) throws SQLException, DatabaseLibraryException {

		DatabaseMetaData dbm = DatabaseConnection.getConnection().getMetaData();
		ResultSet rs = dbm.getTables(null, null, tableName, null);
		try {
			if (!rs.next()) {
				throw new DatabaseLibraryException("Table: " + tableName + " was not found");
			}
		} finally {
			rs.close();
		}
	}
	
	@RobotKeyword("This keyword checks that a given table contains a given amount of rows "
			+ "matching a given WHERE clause. " + "\n\n"
			+ "For the example this means that the table \"MySampleTable\" must contain "
			+ "exactly 2 rows matching the given WHERE, otherwise the teststep will " + "fail. " + "\n\n"
			+ "Example: \n" + "| Verify Number Of Rows Matching Where | MySampleTable | email=x@y.net | 2 | ")
	@ArgumentNames({ "Table to check", "Where clause", "Expected number of rows" })
	public void verifyNumberOfRowsMatchingWhere(String tableName, String where, String rowNumValue)
			throws SQLException, DatabaseLibraryException {

		long rowNum = Long.valueOf(rowNumValue);

		long num = getNumberOfRows(tableName, where, (rowNum + 1));
		if (num != rowNum) {
			throw new DatabaseLibraryException("Expecting " + rowNum + " rows, fetched: " + num);
		}
	}

	private long getNumberOfRows(String tableName) throws SQLException {
		return getNumberOfRows(tableName, Long.MAX_VALUE);
	}
	
	private long getNumberOfRows(String tableName, long limit) throws SQLException {
		return getNumberOfRows(tableName, null, limit);
	}
	
	private long getNumberOfRows(String tableName, String where, long limit) throws SQLException {

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
			Statement stmt = DatabaseConnection.getConnection().createStatement();
			try {
				stmt.executeQuery(sql);
				ResultSet rs = (ResultSet) stmt.getResultSet();
				rs.next();
				num = rs.getLong("count(*)");
			} finally {
				// stmt.close() automatically takes care of its ResultSet, so no
				// rs.close()
				stmt.close();
			}
		} catch (SQLException e) {
			String sql = "select * from " + tableName;
			if (where != null) {
				sql = sql + " where " + where;
			}
			Statement stmt = DatabaseConnection.getConnection().createStatement();
			try {
				stmt.executeQuery(sql);
				ResultSet rs = (ResultSet) stmt.getResultSet();
				num = 0;
				while ((rs.next()) && (num < limit)) {
					num++;
				}
			} finally {
				// stmt.close() automatically takes care of its ResultSet, so no
				// rs.close()
				stmt.close();
			}
		}
		return num;
	}
}
