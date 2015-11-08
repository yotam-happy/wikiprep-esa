package edu.wiki.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class WikiprepESAdb {
	static WikiprepESAdb wikiprepESAdb; 
	Connection connection;

	private WikiprepESAdb() throws ClassNotFoundException, SQLException, FileNotFoundException, IOException {
		// Load the JDBC driver 
		String driverName = "com.mysql.jdbc.Driver"; // MySQL Connector 
		Class.forName(driverName); 
		
		String serverName = WikiprepESAConfiguration.getInstance().getProperty(
				WikiprepESAConfiguration.SERVER_NAME);
		String mydatabase = WikiprepESAConfiguration.getInstance().getProperty(
				WikiprepESAConfiguration.SCHEMA_NAME);
		String username = WikiprepESAConfiguration.getInstance().getProperty(
				WikiprepESAConfiguration.USERNAME);
		String password = WikiprepESAConfiguration.getInstance().getProperty(
				WikiprepESAConfiguration.PASSWORD);

		// Create a connection to the database 
		String url = "jdbc:mysql://" + serverName + "/" + mydatabase; // a JDBC url 
		connection = DriverManager.getConnection(url, username, password);
	}
	
	public static synchronized WikiprepESAdb getInstance() throws ClassNotFoundException, SQLException, FileNotFoundException, IOException {
		if (wikiprepESAdb == null) {
			wikiprepESAdb = new WikiprepESAdb();
		}
		return wikiprepESAdb;
	}
	
	public Connection getConnection() {
		return connection;
	}

}
