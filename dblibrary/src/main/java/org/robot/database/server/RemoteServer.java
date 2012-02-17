package org.robot.database.server;

import java.text.SimpleDateFormat;
import java.util.Date;
import redstone.xmlrpc.simple.*;


public class RemoteServer {

	        
	        public static String className;
	        public static boolean enableStopServer;
		    private static Server server;


	        /** 
	         * Remote Server main/startup method.
	         * Takes input from command line for Java class library (name)
	         * to load and invoke with reflection and the port to bind
	         * the remote server to. Defaults to port 8270 if not supplied.
	         * @param args
	         * 
	         */
	        public static void main(String[] args){// throws Exception {
	                if(args.length == 0) {
	                        displayUsage();
	                }
	                
	                //set defaults
	                int port = 8270;
	                className = "org.robotframework.remotelibrary.ExampleRemoteLibrary";
	                enableStopServer = true;

	                //parse arguments
	                for(int i = 0; i < args.length; i++){
	                    if(args[i].equalsIgnoreCase("--port")) {
	                            port = Integer.parseInt(args[i+1]);
	                    }
	                }       
	                
	                //codebase below based on the Redstone XML-RPC with
	                //Simple HTTP Server integration code example(s)                
	                try
	        {
	            server = new Server(port);
	            server.getXmlRpcServer().addInvocationHandler(null, new RemoteServerMethods());
	            server.start();
	            
	            SimpleDateFormat bartDateFormat = new SimpleDateFormat("MM/dd/yyyy kk:mm");            
	            Date date = new Date();
	            System.out.println("");
	            System.out.println("Database Library remote server started on port " + port + " at " + bartDateFormat.format(date));
	        }
	        catch ( Exception e )
	        {
	            System.out.println(
	                "An exception occured when starting the server. The full stacktrace " +
	                "is displayed below. Check your arguments to verify that the server port " +
	                "is available and that the service classes you supplied are in the classpath " +
	                "and can be instantiated.\n\n" + 
	                "Stacktrace:\n\n" );            
	            e.printStackTrace();
	        }        
	        }  


	        private static void displayUsage() {
	                System.out.println("");
	                System.out.println("robotremoteserver - v1.0");
	                System.out.println("");
	                System.out.println("Usage Info:");
	                System.out.println("");
	                System.out.println("  java -cp xmlrpc-1.1.1.jar:simple-4.0.1.jar:simple-xmlrpc-1.0.jar org.robotframework.remotelibrary.RemoteServer --library RemoteLibraryClassName-FullyQualifiedPath [options]");
	                System.out.println("");
	                System.out.println("  or if everything packaged into a JAR...");
	                System.out.println("");
	                System.out.println("  java -jar jrobotremoteserver-1.0.jar --library RemoteLibraryClassName-FullyQualifiedPath [options]");
	                System.out.println("");
	                System.out.println("  Class name = keyword class w/ methods to execute, fully qualified path");
	                System.out.println("  including package name, as needed. Assumes class library (class/JAR");
	                System.out.println("  file) is in class path for loading & invocation. As shown, server");
	                System.out.println("  requires Redstone XML-RPC w/ Simple HTTP Server (JAR) libraries.");
	                //comment out doc file info, until/unless we implement where some Javadoc file is required
	                //System.out.println("  Documentation file = Java documentation file for the");
	                //System.out.println("    class library.");
	                System.out.println("");
	                //commented out info about hostname/IP until we implement that, if ever
	                //System.out.println("  Optionally specify IP address to bind remote server to.");
	                //System.out.println("    Default of 127.0.0.1 (localhost).");
	                System.out.println("Optional options:");
	                System.out.println("");
	                System.out.println("  --port value\tPort to bind remote server to. Default of 8270.");
	                System.out.println("");
	                System.out.println("  --nostopsvr\tDisable remotely stopping the server via stop_remote_server");
	                System.out.println("             \tkeyword. Default is to enable.");
	                System.out.println("");
	                System.out.println("Example:");
	                System.out.println("");
	                System.out.println("  java -cp xmlrpc-1.1.1.jar:simple-4.0.1.jar:simple-xmlrpc-1.0.jar org.robotframework.remotelibrary.RemoteServer --library org.robotframework.remotelibrary.MyClassName --port 81 --nostopsvr");
	                System.out.println("");
	                System.out.println("  or if everything packaged into a JAR...");
	                System.out.println("");
	                System.out.println("  java -jar jrobotremoteserver-1.0.jar --library org.robotframework.remotelibrary.MyClassName --port 81 --nostopsvr");
	                System.out.println("");
	                System.exit(0);
	        }
	}	