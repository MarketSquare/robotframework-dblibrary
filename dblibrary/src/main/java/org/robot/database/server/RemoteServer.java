package org.robot.database.server;

import java.text.SimpleDateFormat;
import java.util.Date;
import redstone.xmlrpc.simple.*;

public class RemoteServer {

	public static final int DEFAULT_PORT = 8270; 

	public static final String MESSAGE = "Database Library v2.0 remote server started";
	
	private static Server server;

	/**
	 * Remote Server main/startup method. Takes input from command line for Java
	 * class library (name) to load and invoke with reflection and the port to
	 * bind the remote server to. Defaults to port 8270 if not supplied.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		// Setting port and 
		int port = DEFAULT_PORT;

		// Parse command line arguments
		for (int i = 0; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("--port") || args[i].equalsIgnoreCase("-p")) {
				port = Integer.parseInt(args[i + 1]);
			}
			if (args[i].equalsIgnoreCase("--help") || args[i].equalsIgnoreCase("-h")) {
				displayUsage();
				System.exit(0);
			}
		}

		// The actual XML-RPC stuff
		try {
			server = new Server(port);
			server.getXmlRpcServer().addInvocationHandler(null,	new RemoteServerMethods());
			server.start();

			SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm");
			System.out.println(MESSAGE + " on port " + port + " at " + dateFormat.format(new Date()));
		} catch (Exception e) {
			System.out.println("An exception occured when starting the server.\n\n"
					+ "Stacktrace:\n\n");
			e.printStackTrace();
		}
	}

	private static void displayUsage() {
		System.out.println("\n" + MESSAGE + "\n");
		System.out.println("Usage Info:\n");
		System.out.println("Ensure that the dblibrary-2.0-server.jar JAR and your JDBC driver JAR is in the classpath, e.g.:\n");
		System.out.println("set CLASSPATH=%CLASSPATH%;./dblibrary-2.0-server.jar;./mysql-connector-java-5.1.6.jar\n\n");
		System.out.println("The start the server as follows:\n");
		System.out.println("java org.robot.database.server.RemoteServer --port <port> --help");
		System.out.println("");
	}
}