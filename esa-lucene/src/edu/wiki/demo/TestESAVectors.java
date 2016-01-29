package edu.wiki.demo;

import edu.wiki.api.concept.IConceptVector;
import edu.wiki.search.ESASearcher;

public class TestESAVectors extends AbstractTestClass{
	
	public static void main(String[] args) {
		TestESAVectors test = new TestESAVectors();
		test.doMain(args);
	}

	@Override
	public IConceptVector getVector() {
		try {
			ESASearcher searcher = new ESASearcher();
			//BufferedReader in = new BufferedReader(new InputStreamReader(System.in,"UTF-8"));
			String text = "artificial intelligence";//in.readLine();
			
			IConceptVector cvBase = searcher.getConceptVector(text);
			return searcher.getNormalVector(cvBase, MAX_CONCEPTS);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}	
}
