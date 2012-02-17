package org.robot.database.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.robot.database.keywords.DatabaseLibrary;
import org.springframework.remoting.RemoteAccessException;

public class RemoteServerMethods {

	private static final String[] SUPPORTED_METHODS = new String[]{"connect_to_database", 
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
		"execute_sql", "execute_sql_from_file", 
		"execute_sql_from_file_ignore_errors", 
		"verify_number_of_rows_matching_where", 
		"row_should_not_exist_in_table", 
		"test_remote_lib"};
	
	private DatabaseLibrary dbLibrary = new DatabaseLibrary();
	

	public String[] get_keyword_names(){
    	return SUPPORTED_METHODS;
    }
    
    public Map<String, String> run_keyword(String keyword, List<String> args) {

    	Map<String, String> returnRPC = new HashMap<String, String>();
    	
        try {
            	
        	String returnObject = "";
            	
            if (keyword.equals("connect_to_database")) {
            	dbLibrary.connect_to_database(args.get(0), args.get(1), args.get(2), args.get(3));
            } else if (keyword.equals("disconnect_from_database")) {
            	dbLibrary.disconnect_from_database();
            } else if (keyword.equals("table_must_exist")) {
            	dbLibrary.table_must_exist(args.get(0));
            } else if (keyword.equals("table_must_be_empty")) {
            	dbLibrary.table_must_be_empty(args.get(0));
            } else if (keyword.equals("delete_all_rows_from_table")) {
            	dbLibrary.delete_all_rows_from_table(args.get(0));
            } else if (keyword.equals("table_must_contain_number_of_rows")) {
            	dbLibrary.table_must_contain_number_of_rows(args.get(0), args.get(1));
            } else if (keyword.equals("table_must_contain_more_than_number_of_rows")) {
            	dbLibrary.table_must_contain_more_than_number_of_rows(args.get(0), args.get(1));
            } else if (keyword.equals("table_must_contain_less_than_number_of_rows")) {
            	dbLibrary.table_must_contain_less_than_number_of_rows(args.get(0), args.get(1));
            } else if (keyword.equals("tables_must_contain_same_amount_of_rows")) {
            	dbLibrary.tables_must_contain_same_amount_of_rows(args.get(0), args.get(1));
            } else if (keyword.equals("check_content_for_row_identified_by_rownum")) {
            	dbLibrary.check_content_for_row_identified_by_rownum(args.get(0), args.get(1), args.get(2), args.get(3));
            } else if (keyword.equals("check_content_for_row_identified_by_where_clause")) {
            	dbLibrary.check_content_for_row_identified_by_where_clause(args.get(0), args.get(1), args.get(2), args.get(3));
            } else if (keyword.equals("read_single_value_from_table")) {
            	dbLibrary.read_single_value_from_table(args.get(0), args.get(1), args.get(2));
            } else if (keyword.equals("transaction_isolation_level_must_be")) {
            	dbLibrary.transaction_isolation_level_must_be(args.get(0));
            } else if (keyword.equals("get_transaction_isolation_level")) {
            	dbLibrary.get_transaction_isolation_level();
            } else if (keyword.equals("check_primary_key_columns_for_table")) {
            	dbLibrary.check_primary_key_columns_for_table(args.get(0), args.get(1));
            } else if (keyword.equals("get_primary_key_columns_for_table")) {
            	dbLibrary.get_primary_key_columns_for_table(args.get(0));
            } else if (keyword.equals("execute_sql")) {
            	dbLibrary.execute_sql(args.get(0));
            } else if (keyword.equals("execute_sql_from_file")) {
            	dbLibrary.execute_sql_from_file(args.get(0));
            } else if (keyword.equals("execute_sql_from_file_ignore_errors")) {
            	dbLibrary.execute_sql_from_file_ignore_errors(args.get(0));
            } else if (keyword.equals("verify_number_of_rows_matching_where")) {
            	dbLibrary.verify_number_of_rows_matching_where(args.get(0), args.get(1), args.get(2));
            } else if (keyword.equals("row_should_not_exist_in_table")) {
            	dbLibrary.row_should_not_exist_in_table(args.get(0), args.get(1));
            } else if (keyword.equals("test_remote_lib")) {
            	dbLibrary.test_remote_lib(args.get(0));
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
        } else if (keyword.equals("test_remote_lib")) {
        	argumentList = new String[]{"value"};
        } else {
        	argumentList = new String[]{};
        }

        return argumentList;
    }
    
    public String get_keyword_documentation(String keyword){
    	return "empty";
    }
}
