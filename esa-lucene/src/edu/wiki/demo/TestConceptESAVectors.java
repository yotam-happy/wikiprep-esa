package edu.wiki.demo;

import java.io.IOException;
import java.sql.SQLException;

import edu.wiki.api.concept.IConceptVector;
import edu.wiki.search.ESASearcher;
import edu.wiki.util.WikiprepESAdb;

public class TestConceptESAVectors  extends AbstractTestClass{
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		TestCombinedESAVectors test = new TestCombinedESAVectors();
		test.doMain(args);
	}

	@Override
	public IConceptVector getVector() {
		try {
			ESASearcher searcher = new ESASearcher();
			//BufferedReader in = new BufferedReader(new InputStreamReader(System.in,"UTF-8"));
			//Integer id = Integer.decode(in.readLine());
			Integer id = 12; // = artificial intelligence

			connection = WikiprepESAdb.getInstance().getConnection();
			stmtQuery = connection.createStatement();
			
			return searcher.getConceptESAVector(id);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}	
}
