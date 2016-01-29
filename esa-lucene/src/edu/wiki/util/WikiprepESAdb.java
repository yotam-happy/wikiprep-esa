package edu.wiki.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class WikiprepESAdb {
	static WikiprepESAdb wikiprepESAdb; 
	Connection connection;

	private WikiprepESAdb() {
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
		try {
			connection = DriverManager.getConnection(url, username, password);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static synchronized WikiprepESAdb getInstance() {
		if (wikiprepESAdb == null) {
			wikiprepESAdb = new WikiprepESAdb();
		}
		return wikiprepESAdb;
	}
	
	public Connection getConnection() {
		return connection;
	}

}
