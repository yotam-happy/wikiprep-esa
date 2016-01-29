package edu.wiki.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import edu.wiki.api.concept.IConceptVector;
import edu.wiki.search.ESASearcher;

public class TestCombinedESAVectors extends AbstractTestClass {
	
	public static void main(String[] args) {
		TestCombinedESAVectors test = new TestCombinedESAVectors();
		test.doMain(args);
	}

	@Override
	public IConceptVector getVector() {
		String text;
		ESASearcher searcher = new ESASearcher();
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in,"UTF-8"));
			text = in.readLine();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		text = "artificial intelligence"; //in.readLine();
		
		return searcher.getCombinedVector(text, MAX_CONCEPTS);
	}	
}
