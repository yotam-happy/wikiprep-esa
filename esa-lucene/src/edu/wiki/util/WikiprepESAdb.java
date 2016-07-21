package edu.wiki.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

import edu.wiki.util.counting.Counting;

public class WikiprepESAdb {
	static WikiprepESAdb wikiprepESAdb; 
	Connection connection;

	private WikiprepESAdb() {
		connection = getNewConnection();
	}
	
	public static Connection getNewConnection(){
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
			return DriverManager.getConnection(url, username, password);
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
	
	/**
	 * This method uses jdbc api to read the records one by one. This has the effect of blocking
	 * the connections for further queries until this operation finishes so be warned 
	 */
	public void forEachResult(String query, Consumer<ResultSet> consumer){
		try {
			Statement stmt = WikiprepESAdb.getInstance().getConnection().createStatement();
			stmt = WikiprepESAdb.getInstance().getConnection()
					.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(Integer.MIN_VALUE);
			
			stmt.execute(query);
			ResultSet rs = stmt.getResultSet();
			Counting c = new Counting(10);
			while(rs.next()) {
				consumer.accept(rs);
				c.addOne();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

}
