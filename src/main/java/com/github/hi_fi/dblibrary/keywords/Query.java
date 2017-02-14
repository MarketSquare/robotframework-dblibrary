package com.github.hi_fi.dblibrary.keywords;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.robotframework.javalib.annotation.ArgumentNames;
import org.robotframework.javalib.annotation.RobotKeyword;
import org.robotframework.javalib.annotation.RobotKeywords;

@RobotKeywords
public class Query {
	@RobotKeyword("Executes the given SQL without any further modifications. The given SQL "
			+ "must be valid for the database that is used. Results are returned as a list of dictionaries" + "\n\n"
			+ "*NOTE*: Use this method with care as you might cause damage to your "
			+ "database, especially when using this in a productive environment. " + "\n\n" + "Example: \n"
			+ "| Execute SQL | CREATE TABLE MyTable (Num INTEGER) | ")
	@ArgumentNames({ "SQL String to execute" })
	public List<HashMap<String, Object>> executeSql(String sqlString) throws SQLException {
		ArrayList<HashMap<String, Object>> list = new ArrayList<HashMap<String, Object>>();
		Statement stmt = DatabaseConnection.getConnection().createStatement();
		try {
			stmt.execute(sqlString);
			ResultSet rs = (ResultSet) stmt.getResultSet();
			if (rs != null) {
				ResultSetMetaData rsmd = rs.getMetaData();
				int numberOfColumns = rsmd.getColumnCount();
				while (rs.next()) {
					HashMap<String, Object> row = new HashMap<String, Object>(numberOfColumns);
					for (int i = 1; i <= numberOfColumns; ++i) {
						row.put(rsmd.getColumnName(i), rs.getObject(i));
					}
					list.add(row);
				}
			}
		} finally {
			stmt.close();
		}

		return list;
	}

	@RobotKeyword("Executes the SQL statements contained in the given file without any "
			+ "further modifications. The given SQL must be valid for the database that "
			+ "is used. Any lines prefixed with \"REM\" or \"#\" are ignored. This keyword "
			+ "can for example be used to setup database tables from some SQL install " + "script. " + "\n\n"
			+ "Single SQL statements in the file can be spread over multiple lines, but "
			+ "must be terminated with a semicolon \";\". A new statement must always "
			+ "start in a new line and not in the same line where the previous statement "
			+ "was terminated by a \";\". " + "\n\n"
			+ "In case there is a problem in executing any of the SQL statements from "
			+ "the file the execution is terminated and the operation is rolled back. " + "\n\n"
			+ "*NOTE*: Use this method with care as you might cause damage to your "
			+ "database, especially when using this in a productive environment. \n\n"
			+ "*NOTE2*: If using keyword remotely, file need to be trasfered to server some "
			+ "other way; this library is not doing the transfer." + "\n\n" + "Example: \n"
			+ "| Execute SQL from File | myFile.sql | ")
	@ArgumentNames({ "File containing SQL commands to execute" })
	public void executeSqlFromFile(String fileName) throws SQLException, IOException, DatabaseLibraryException {

		DatabaseConnection.getConnection().setAutoCommit(false);

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
				DatabaseConnection.getConnection().rollback();
				DatabaseConnection.getConnection().setAutoCommit(true);
				throw new DatabaseLibraryException("Error executing: " + sql + " Execution from file rolled back!");
			}
		}

		DatabaseConnection.getConnection().commit();
		DatabaseConnection.getConnection().setAutoCommit(true);
		br.close();
	}

	@RobotKeyword("Executes the SQL statements contained in the given file without any "
			+ "further modifications. The given SQL must be valid for the database that "
			+ "is used. Any lines prefixed with \"REM\" or \"#\" are ignored. This keyword "
			+ "can for example be used to setup database tables from some SQL install " + "script. " + "\n\n"
			+ "Single SQL statements in the file can be spread over multiple lines, but "
			+ "must be terminated with a semicolon \";\". A new statement must always "
			+ "start in a new line and not in the same line where the previous statement "
			+ "was terminated by a \";\". " + "\n\n"
			+ "Any errors that might happen during execution of SQL statements are "
			+ "logged to the Robot Log-file, but otherwise ignored. " + "\n\n"
			+ "*NOTE*: Use this method with care as you might cause damage to your "
			+ "database, especially when using this in a productive environment. \n\n"
			+ "*NOTE2*: If using keyword remotely, file need to be trasfered to server some "
			+ "other way; this library is not doing the transfer." + "\n\n" + "Example: \n"
			+ "| Execute SQL from File | myFile.sql | ")
	@ArgumentNames({ "File containing SQL commands to execute" })
	public void executeSqlFromFileIgnoreErrors(String fileName)
			throws SQLException, IOException, DatabaseLibraryException {

		DatabaseConnection.getConnection().setAutoCommit(false);

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
				System.out.println("Error executing: " + sql + "\n" + e.getMessage() + "\n\n");
				sql = "";
			}
		}

		DatabaseConnection.getConnection().commit();
		DatabaseConnection.getConnection().setAutoCommit(true);
		br.close();
	}


}
