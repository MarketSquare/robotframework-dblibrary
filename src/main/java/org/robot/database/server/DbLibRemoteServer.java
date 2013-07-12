package org.robot.database.server;



import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import org.apache.commons.io.IOUtils;
import org.robot.database.keywords.DatabaseLibrary;
import org.robotframework.javalib.library.AnnotationLibrary;
import org.robotframework.remoteserver.RemoteServer;

public class DbLibRemoteServer extends AnnotationLibrary {

	public static final int DEFAULT_PORT = 8270; 

	public static final String MESSAGE = "Database Library v2.1 remote server started";
	

	public DbLibRemoteServer() {
		 super("org/robot/database/keywords/DatabaseLibrary.class");
	}
	
	
	@Override
	public String getKeywordDocumentation(String keywordName) {
		if (keywordName.equals("__intro__")) {
			return getIntro();
		}
	       
		return super.getKeywordDocumentation(keywordName);
	}
	
	
	public static void main(String[] args) throws Exception {
		
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
		
		RemoteServer.configureLogging();
        RemoteServer server = new RemoteServer();
        server.addLibrary(DatabaseLibrary.class, port);
        server.start();	
    }


    private String getIntro() {
        try {
            InputStream introStream = DbLibRemoteServer.class.getResourceAsStream("__intro__.txt");
            StringWriter writer = new StringWriter();
            IOUtils.copy(introStream, writer);
            return writer.toString();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

	
	private static void displayUsage() {
		System.out.println("\n" + MESSAGE + "\n");
		System.out.println("Usage Info:\n");
		System.out.println("Ensure that the dblibrary-2.1-server.jar JAR and your JDBC driver JAR is in the classpath, e.g.:\n");
		System.out.println("set CLASSPATH=%CLASSPATH%;./dblibrary-2.0-server.jar;./mysql-connector-java-5.1.6.jar\n\n");
		System.out.println("The start the server as follows:\n");
		System.out.println("java org.robot.database.server.DbLibRemoteServer --port <port> --help");
		System.out.println("");
	}
}