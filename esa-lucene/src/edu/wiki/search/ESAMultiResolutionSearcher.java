package edu.wiki.search;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.analysis.CustomTokenizer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.concept.TroveConceptVector;

/**
 * Builds an ESA vector for a document based on multi resolution contexts 
 * as described in Gabrilovich et al. (2009) 
 * @author yotamesh
 *
 */
public class ESAMultiResolutionSearcher extends ESASearcher {

	public ESAMultiResolutionSearcher() throws ClassNotFoundException, IOException {
		super();
	}

	public IConceptVector getConceptVectorUsingMultiResolution(String doc, int conceptsLimit, boolean use2ndOrder) throws IOException {
		// Compute by contexts: document resolution, 50 word resolution and 10 word resolution
		Set<String> contexts = new HashSet<String>();
		contexts.add(doc);
		contexts.addAll(getWindowOfWordsContexts(doc, 50));
		contexts.addAll(getWindowOfWordsContexts(doc, 10));
		
		Map<Integer,Double> result = new HashMap<Integer, Double>();

		// Get k best describing concepts from each context
		for (String context : contexts) {
			IConceptVector v = use2ndOrder ? getCombinedVector(context) : getConceptVector(context);
			IConceptIterator iter = v.orderedIterator();
			int k = 5;
			while (iter.next() && k > 0) {
				int conceptId = iter.getId();
				double conceptScore = iter.getValue();
				if (!result.containsKey(conceptId) ||
						result.get(conceptId) < conceptScore) {
					result.put(conceptId, conceptScore);
				}
				k--;
			}
		}
		
		// build final vector
		IConceptVector vec = new TroveConceptVector(result.size());
		for (Entry<Integer,Double> e : result.entrySet()) {
			vec.add(e.getKey(), e.getValue());
		}
		vec = getNormalVector(vec, conceptsLimit);
		return vec;
	}
	
	public Set<String> getWindowOfWordsContexts(String doc, int sz) throws IOException {
		Set<String> result = new HashSet<String>();
		CustomTokenizer wordTokenizer = new CustomTokenizer(new StringReader(doc));

		StringBuffer window = new StringBuffer();
		int count = 0;
		while (wordTokenizer.incrementToken()) {
			window.append(" ").append(wordTokenizer.getAttribute(TermAttribute.class).term());
			count++;
			if (count >= sz) {
				result.add(window.toString());
				window = new StringBuffer();
				count = 0;
			}
		}
		if (count > 0) {
			result.add(window.toString());
		}
		wordTokenizer.close();
		return result;
	}
}