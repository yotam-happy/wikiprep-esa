package edu.wiki.demo;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.util.WikiprepESAdb;

public abstract class AbstractTestClass {
	static Connection connection;
	static Statement stmtQuery;
	
	static String strTitles = "SELECT id,title FROM article WHERE id IN ";
	static final int MAX_CONCEPTS = 20;
	
	public static void initDB() {
		connection = WikiprepESAdb.getInstance().getConnection();
		try {
			stmtQuery = connection.createStatement();
			stmtQuery.setFetchSize(100);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
  }

	public abstract IConceptVector getVector();

	public void doMain(String[] args) {
		initDB();

		IConceptVector cv = getVector();
		
		if(cv == null){
			System.exit(1);
		}
		
		IConceptIterator it = cv.orderedIterator();
		
		HashMap<Integer, Double> vals = new HashMap<Integer, Double>(10);
		HashMap<Integer, String> titles = new HashMap<Integer, String>(10);
		
		String inPart = "(";
		
		int count = 0;
		while(it.next() && count < MAX_CONCEPTS){
			inPart += it.getId() + ",";
			vals.put(it.getId(),it.getValue());
			count++;
		}
		
		inPart = inPart.substring(0,inPart.length()-1) + ")";

		try{
			ResultSet r = stmtQuery.executeQuery(strTitles + inPart);
			while(r.next()){
				titles.put(r.getInt(1), new String(r.getBytes(2),"UTF-8")); 
			}
		}catch(SQLException | UnsupportedEncodingException e){
			throw new RuntimeException(e);
		}
		
		count = 0;
		it.reset();
		while(it.next() && count < MAX_CONCEPTS){
			int id = it.getId();
			System.out.println(id + "\t\t" + titles.get(id) + "\t\t" + vals.get(id));
			count++;
		}
		

	}


}
