package edu.wiki.util;

import java.util.ArrayList;
import java.util.Collection;

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

}
