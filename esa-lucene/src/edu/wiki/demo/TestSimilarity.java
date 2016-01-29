package edu.wiki.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

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
			System.out.println(searcher.getRelatedness(doc1, doc2));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
