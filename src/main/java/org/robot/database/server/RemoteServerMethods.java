package org.robot.database.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.robot.database.keywords.DatabaseLibrary;
import org.springframework.remoting.RemoteAccessException;

public class RemoteServerMethods {

	/**
	 * Define available methods.
	 */
	private static final String[] SUPPORTED_METHODS = new String[]{
		"connect_to_database", 
		"disconnect_from_database", 
		"table_must_exist", 
		"table_must_be_empty", 
		"delete_all_rows_from_table", 
		"table_must_contain_number_of_rows", 
		"table_must_contain_more_than_number_of_rows", 
		"table_must_contain_less_than_number_of_rows", 
		"tables_must_contain_same_amount_of_rows", 
		"check_content_for_row_identified_by_rownum", 
		"check_content_for_row_identified_by_where_clause", 
		"read_single_value_from_table", 
		"transaction_isolation_level_must_be", 
		"get_transaction_isolation_level", 
		"check_primary_key_columns_for_table", 
		"get_primary_key_columns_for_table", 
		"execute_sql", 
		"execute_sql_from_file", 
		"execute_sql_from_file_ignore_errors", 
		"verify_number_of_rows_matching_where", 
		"row_should_not_exist_in_table",
		"store_query_result_to_file",
		"compare_query_result_to_file"};
	
	private DatabaseLibrary dbLibrary = new DatabaseLibrary();
	

	/**
	 * Returns the list of supported keywords.
	 * @return Array containing keywords.
	 */
	public String[] get_keyword_names(){
    	return SUPPORTED_METHODS;
    }

	/**
	 * Executed the given keyword and returns the result.
	 * @param keyword Name of the keyword.
	 * @param args Arguments-Array
	 * @return Result-Map
	 */
    public Map<String, String> run_keyword(String keyword, List<String> args) {

    	Map<String, String> returnRPC = new HashMap<String, String>();
    	
        try {
            	
        	String returnObject = "";
            	
            if (keyword.equals("connect_to_database")) {
            	dbLibrary.connectToDatabase(args.get(0), args.get(1), args.get(2), args.get(3));
            } else if (keyword.equals("disconnect_from_database")) {
            	dbLibrary.disconnectFromDatabase();
            } else if (keyword.equals("table_must_exist")) {
            	dbLibrary.tableMustExist(args.get(0));
            } else if (keyword.equals("table_must_be_empty")) {
            	dbLibrary.tableMustBeEmpty(args.get(0));
            } else if (keyword.equals("delete_all_rows_from_table")) {
            	dbLibrary.deleteAllRowsFromTable(args.get(0));
            } else if (keyword.equals("table_must_contain_number_of_rows")) {
            	dbLibrary.tableMustContainNumberOfRows(args.get(0), args.get(1));
            } else if (keyword.equals("table_must_contain_more_than_number_of_rows")) {
            	dbLibrary.tableMustContainMoreThanNumberOfRows(args.get(0), args.get(1));
            } else if (keyword.equals("table_must_contain_less_than_number_of_rows")) {
            	dbLibrary.tableMustContainLessThanNumberOfRows(args.get(0), args.get(1));
            } else if (keyword.equals("tables_must_contain_same_amount_of_rows")) {
            	dbLibrary.tablesMustContainSameAmountOfRows(args.get(0), args.get(1));
            } else if (keyword.equals("check_content_for_row_identified_by_rownum")) {
            	dbLibrary.checkContentForRowIdentifiedByRownum(args.get(0), args.get(1), args.get(2), args.get(3));
            } else if (keyword.equals("check_content_for_row_identified_by_where_clause")) {
            	dbLibrary.checkContentForRowIdentifiedByWhereClause(args.get(0), args.get(1), args.get(2), args.get(3));
            } else if (keyword.equals("read_single_value_from_table")) {
            	returnObject = dbLibrary.readSingleValueFromTable(args.get(0), args.get(1), args.get(2));
            } else if (keyword.equals("transaction_isolation_level_must_be")) {
            	dbLibrary.transactionIsolationLevelMustBe(args.get(0));
            } else if (keyword.equals("get_transaction_isolation_level")) {
            	returnObject = dbLibrary.getTransactionIsolationLevel();
            } else if (keyword.equals("check_primary_key_columns_for_table")) {
            	dbLibrary.checkPrimaryKeyColumnsForTable(args.get(0), args.get(1));
            } else if (keyword.equals("get_primary_key_columns_for_table")) {
            	returnObject = dbLibrary.getPrimaryKeyColumnsForTable(args.get(0));
            } else if (keyword.equals("execute_sql")) {
            	dbLibrary.executeSql(args.get(0));
            } else if (keyword.equals("execute_sql_from_file")) {
            	dbLibrary.executeSqlFromFile(args.get(0));
            } else if (keyword.equals("execute_sql_from_file_ignore_errors")) {
            	dbLibrary.executeSqlFromFileIgnoreErrors(args.get(0));
            } else if (keyword.equals("verify_number_of_rows_matching_where")) {
            	dbLibrary.verifyNumberOfRowsMatchingWhere(args.get(0), args.get(1), args.get(2));
            } else if (keyword.equals("row_should_not_exist_in_table")) {
            	dbLibrary.rowShouldNotExistInTable(args.get(0), args.get(1));
            } else if (keyword.equals("store_query_result_to_file")) {
            	dbLibrary.storeQueryResultToFile(args.get(0), args.get(1));
            } else if (keyword.equals("compare_query_result_to_file")) {
            	dbLibrary.compareQueryResultToFile(args.get(0), args.get(1));
            } else {
            	throw new RemoteAccessException("The keyword " + keyword + "does not exist in the Database Library Remote Server.");
            }
            	
            returnRPC.put("error","");
            returnRPC.put("output","");
            returnRPC.put("traceback","");
            returnRPC.put("status", "PASS");  
            returnRPC.put("return", returnObject);

        } catch (Throwable e) {

        	e.printStackTrace();
        	returnRPC.put("status", "FAIL");
        	returnRPC.put("return", "");
        	returnRPC.put("error",e.getMessage());
        	returnRPC.put("output",e.getMessage());

        	String stktrc = "";
            for (StackTraceElement element :  e.getStackTrace()) {
                stktrc += element.toString();
                returnRPC.put("traceback",stktrc);
            }
        }
        
        return returnRPC;
    }


    /**
     * Returns the arguments required by the given keyword.
     * @param keyword Keyword
     * @return Argument-Array
     */
    public String[] get_keyword_arguments(String keyword){
    	
    	String[] argumentList;
    	
        if (keyword.equals("connect_to_database")) {
        	argumentList = new String[]{"driverClassName", "connectString", "dbUser", "dbPassword"};
        } else if (keyword.equals("disconnect_from_database")) {
        	argumentList = new String[]{};
        } else if (keyword.equals("table_must_exist")) {
        	argumentList = new String[]{"tableName"};
        } else if (keyword.equals("table_must_be_empty")) {
        	argumentList = new String[]{"tableName"};
        } else if (keyword.equals("delete_all_rows_from_table")) {
        	argumentList = new String[]{"tableName"};
        } else if (keyword.equals("table_must_contain_number_of_rows")) {
        	argumentList = new String[]{"tableName", "rowNumValue"};
        } else if (keyword.equals("table_must_contain_more_than_number_of_rows")) {
        	argumentList = new String[]{"tableName", "rowNumValue"};
        } else if (keyword.equals("table_must_contain_less_than_number_of_rows")) {
        	argumentList = new String[]{"tableName", "rowNumValue"};
        } else if (keyword.equals("tables_must_contain_same_amount_of_rows")) {
        	argumentList = new String[]{"firstTableName", "secondTableName"};
        } else if (keyword.equals("check_content_for_row_identified_by_rownum")) {
        	argumentList = new String[]{"columnNames", "expectedValues", "tableName", "rowNumValue"};
        } else if (keyword.equals("check_content_for_row_identified_by_where_clause")) {
        	argumentList = new String[]{"columnNames", "expectedValues", "tableName", "whereClause"};
        } else if (keyword.equals("read_single_value_from_table")) {
        	argumentList = new String[]{"tableName", "columnName", "whereClause"};
        } else if (keyword.equals("transaction_isolation_level_must_be")) {
        	argumentList = new String[]{"levelName"};
        } else if (keyword.equals("get_transaction_isolation_level")) {
        	argumentList = new String[]{};
        } else if (keyword.equals("check_primary_key_columns_for_table")) {
        	argumentList = new String[]{"tableName", "columnList"};
        } else if (keyword.equals("get_primary_key_columns_for_table")) {
        	argumentList = new String[]{"tableName"};
        } else if (keyword.equals("execute_sql")) {
        	argumentList = new String[]{"sqlString"};
        } else if (keyword.equals("execute_sql_from_file")) {
        	argumentList = new String[]{"fileName"};
        } else if (keyword.equals("execute_sql_from_file_ignore_errors")) {
        	argumentList = new String[]{"fileName"};
        } else if (keyword.equals("verify_number_of_rows_matching_where")) {
        	argumentList = new String[]{"tableName", "where", "rowNumValue"};
        } else if (keyword.equals("row_should_not_exist_in_table")) {
        	argumentList = new String[]{"tableName", "whereClause"};
        } else if (keyword.equals("store_query_result_to_file")) {
        	argumentList = new String[]{"sqlString", "fileName"};
        } else if (keyword.equals("compare_query_result_to_file")) {
        	argumentList = new String[]{"sqlString", "fileName"};
        } else {
        	argumentList = new String[]{};
        }

        return argumentList;
    }
    
    /**
     * Returns the documentation for the given keyword.
     * @param keyword Keyword
     * @return Documentation
     */
    public String get_keyword_documentation(String keyword){
    	
    	if (DatabaseLibrary.documentation.containsValue(keyword)) {
    		return DatabaseLibrary.documentation.get(keyword);
    	} else {
    		return "Sorry, no documentation available.";
    	}
    }
}
