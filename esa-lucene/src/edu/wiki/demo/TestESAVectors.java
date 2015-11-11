package edu.wiki.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.search.ESASearcher;
import edu.wiki.util.WikiprepESAdb;

public class TestESAVectors extends AbstractTestClass{
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		TestCombinedESAVectors test = new TestCombinedESAVectors();
		test.doMain(args);
	}

	@Override
	public IConceptVector getVector() {
		try {
			ESASearcher searcher = new ESASearcher();
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in,"UTF-8"));
			String text = "artificial intelligence";//in.readLine();
			
			IConceptVector cvBase = searcher.getConceptVector(text);
			return searcher.getNormalVector(cvBase, MAX_CONCEPTS);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}	
}
