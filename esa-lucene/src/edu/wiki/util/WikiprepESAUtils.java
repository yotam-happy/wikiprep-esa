package edu.wiki.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.CustomTokenizer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

public class WikiprepESAUtils {
	/**
	 * This method divides a wikipedia document found in the text table into paragraphs
	 * 
	 * Wikipedia has each paragraph in a single line. wikiprep keeps this information
	 * The devision into paragraphs is pretty well formed throughout wikipedia so they make
	 * natural candidates for 'contexts'
	 * @param doc
	 * @return
	 */
	public static Collection<String> getWikipediaDocumentParagraph(String doc) {
		Collection<String> contexts = new ArrayList<>();
		int i =  0;
		int j = doc.indexOf('\n');
		while (j >= 0) {
			if (j - i > 100) {
				contexts.add(doc.substring(i, j));
			}
			i = j + 1;
			j = doc.indexOf('\n', i);
		}
		return contexts;
	}
	
	public static Set<String> getWindowOfWordsContexts(String doc, int sz, int overlapSz) {
		return getWindowOfWordsContexts(Arrays.asList(doc), sz, overlapSz);
	}
	public static Set<String> getWindowOfWordsContexts(Collection<String> largerContexts, int sz, int overlapSz) {
		Set<String> result = new HashSet<String>();

		largerContexts.forEach((context) -> {
			try {
				CustomTokenizer wordTokenizer = new CustomTokenizer(new StringReader(context));
	
				StringBuffer nextWindow = new StringBuffer();
				StringBuffer window = new StringBuffer();
				int count = 0;
				while (wordTokenizer.incrementToken()) {
					window.append(" ").append(wordTokenizer.getAttribute(TermAttribute.class).term());
					if (sz - count >= overlapSz){
						nextWindow.append(" ").append(wordTokenizer.getAttribute(TermAttribute.class).term());
					}
					count++;
					if (count >= sz) {
						result.add(window.toString());
						window = nextWindow;
						nextWindow = new StringBuffer();
						count = overlapSz;
					}
				}
				if (count > 0) {
					result.add(window.toString());
				}
				wordTokenizer.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
		return result;
	}

}
