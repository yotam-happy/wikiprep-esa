package edu.wiki.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import edu.wiki.api.concept.IConceptVector;
import edu.wiki.search.ESASearcher;

public class TestSimilarity {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ESASearcher searcher = new ESASearcher();
		
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in,"UTF-8"));
			String doc1 = br.readLine();
			String doc2 = br.readLine();
			br.close();
			IConceptVector v1 = searcher.getCombinedVector(doc1, 1000);
			IConceptVector v2 = searcher.getCombinedVector(doc2, 1000);
			System.out.println(searcher.getRelatedness(v1,v2));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
