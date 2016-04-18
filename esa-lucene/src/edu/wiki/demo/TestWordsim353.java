package edu.wiki.demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;

import edu.wiki.api.concept.IConceptVector;
import edu.wiki.search.ESASearcher;

public class TestWordsim353 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ESASearcher searcher = new ESASearcher();
		String line;
		double val;
		DecimalFormat df = new DecimalFormat("#.##########");

		try {
			// read Wordsim-353 human judgements
			FileInputStream fis = new FileInputStream(new File("config/wordsim353-combined.tab"));
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			br.readLine(); //skip first line
			System.out.println("Word 1\tWord 2\tHuman (mean)\tScore");
			while((line = br.readLine()) != null){
				final String [] parts = line.split("\t");
				if(parts.length != 3)
					break;
				
				IConceptVector v1 = searcher.getConceptVector(parts[0]);
				IConceptVector v2 = searcher.getConceptVector(parts[1]);
				val = searcher.getRelatedness(v1, v2);
				
				if(val == -1){
					System.out.println(line + "\t0");
				}
				else {
					System.out.println(line + "\t" + df.format(val));
				}
			}
			br.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
	}

}
