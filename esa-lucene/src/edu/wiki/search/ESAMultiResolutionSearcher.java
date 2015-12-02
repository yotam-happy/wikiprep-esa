package edu.wiki.search;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;





import org.apache.lucene.analysis.CustomTokenizer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;





import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.concept.TroveConceptVector;
import edu.wiki.util.WikiprepESAUtils;
import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntDoubleIterator;

/**
 * Builds an ESA vector for a document based on multi resolution contexts 
 * as described in Gabrilovich et al. (2009) 
 * @author yotamesh
 *
 */
public class ESAMultiResolutionSearcher extends ESASearcher {

	public static final int LARGE_WINDOW_OF_WORDS_WIDTH = 150;
	public static final int WINDOW_OF_WORDS_WIDTH = 15;
	public static final int CONCEPTS_PER_SHORT_CONTEXT = 5;
	public static final int CONCEPTS_PER_MED_CONTEXT = 10;
	public static final int CONCEPTS_PER_LONG_CONTEXT = 25;
	
	public ESAMultiResolutionSearcher() throws ClassNotFoundException, IOException {
		super();
	}

	public IConceptVector getConceptVectorUsingMultiResolution(String doc, int conceptsLimit, boolean use2ndOrder, boolean useShortContexts) {
		TIntDoubleHashMap result = new TIntDoubleHashMap();
		
		// get concepts from analyzing entire doc
		getConceptVectorUsingMultiResolutionInternal(doc, use2ndOrder, CONCEPTS_PER_LONG_CONTEXT, result);

		// get concepts from analyzing paragraphs (approximated by using large window of words contexts)
		Collection<String> paragraphs = getWindowOfWordsContexts(Arrays.asList(doc), LARGE_WINDOW_OF_WORDS_WIDTH);
		paragraphs.forEach((paragraph) -> {
			getConceptVectorUsingMultiResolutionInternal(paragraph, use2ndOrder, CONCEPTS_PER_MED_CONTEXT, result);
		});

		// get concepts from analyzing window of words contexts (within paragraphs)
		getWindowOfWordsContexts(paragraphs, WINDOW_OF_WORDS_WIDTH).forEach((wow) -> {
			getConceptVectorUsingMultiResolutionInternal(wow, use2ndOrder, CONCEPTS_PER_SHORT_CONTEXT, result);
		});
		
		// build final vector
		IConceptVector vec = new TroveConceptVector(result.size());
		TIntDoubleIterator iter = result.iterator();
		while(iter.hasNext()) {
			iter.advance();
			vec.set(iter.key(), iter.value());
		}
		vec = getNormalVector(vec, conceptsLimit);
		return vec;
	}

	public IConceptVector getConceptVectorUsingMultiResolutionForWikipedia(String doc, int conceptsLimit, boolean use2ndOrder, boolean useShortContexts) {
		TIntDoubleHashMap result = new TIntDoubleHashMap();
		
		// get concepts from analyzing entire doc
		getConceptVectorUsingMultiResolutionInternal(doc, use2ndOrder, CONCEPTS_PER_LONG_CONTEXT, result);

		// get concepts from analyzing individual paragraph contexts
		Collection<String> paragraphs = WikiprepESAUtils.getWikipediaDocumentParagraph(doc);
		paragraphs.forEach((paragraph) -> {
			getConceptVectorUsingMultiResolutionInternal(paragraph, use2ndOrder, CONCEPTS_PER_MED_CONTEXT, result);
		});

		// get concepts from analyzing window of words contexts (within paragraphs
		getWindowOfWordsContexts(paragraphs, WINDOW_OF_WORDS_WIDTH).forEach((wow) -> {
			getConceptVectorUsingMultiResolutionInternal(wow, use2ndOrder, CONCEPTS_PER_SHORT_CONTEXT, result);
		});
		
		// build final vector
		IConceptVector vec = new TroveConceptVector(result.size());
		TIntDoubleIterator iter = result.iterator();
		while(iter.hasNext()) {
			iter.advance();
			vec.set(iter.key(), iter.value());
		}
		vec = getNormalVector(vec, conceptsLimit);
		return vec;
	}
	
	private void getConceptVectorUsingMultiResolutionInternal(
			String context, 
			boolean use2ndOrder, 
			int maxConcepts,
			TIntDoubleHashMap result) {
		IConceptVector v;
		try {
			v = use2ndOrder ? getCombinedVector(context, maxConcepts) : getNormalVector(context, maxConcepts);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		if (v == null || v.count() == 0) {
			return;
		}
		
		IConceptIterator iter = v.bestKOrderedIterator(maxConcepts);
		while (iter.next()) {
			int conceptId = iter.getId();
			double conceptScore = iter.getValue();
			if (!result.containsKey(conceptId) ||
					result.get(conceptId) < conceptScore) {
				result.put(conceptId, conceptScore);
			}
		}
	}
	
	public Set<String> getWindowOfWordsContexts(Collection<String> largerContexts, int sz) {
		Set<String> result = new HashSet<String>();

		largerContexts.forEach((context) -> {
			try {
				CustomTokenizer wordTokenizer = new CustomTokenizer(new StringReader(context));
	
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
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
		return result;
	}
}