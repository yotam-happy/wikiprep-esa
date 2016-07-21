package edu.wiki.search;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.concept.ConceptVectorCosineSimilarity;
import edu.wiki.concept.TroveConceptVector;
import edu.wiki.index.WikipediaAnalyzer;
import edu.wiki.util.InplaceSorts;
import edu.wiki.util.TermVectorIterator;
import edu.wiki.util.db.Concept2ndOrderQueryOptimizer;
import edu.wiki.util.db.ConceptESAVectorQueryOptimizer;
import edu.wiki.util.db.TermQueryOptimizer;
import gnu.trove.THashMap;
import gnu.trove.THashSet;
import gnu.trove.TIntDoubleHashMap;

/**
 * Performs search on the index located in database.
 * 
 * @author Cagatay Calli <ccalli@gmail.com>
 */
public class ESASearcher {
	WikipediaAnalyzer analyzer;
	
	static final float LINK_ALPHA = 0.5f;
	
	public ESASearcher() {
		analyzer = new WikipediaAnalyzer();
	}
	
	public IConceptVector getConceptVector(String query) {
        IConceptVector newCv = new TroveConceptVector(30);
        getConceptVectorInternal(query, (id,s)->newCv.add(id, s));
        return newCv;
	}
	
	double[] tmpArray = null;
	static final int tmpArraySize = 1500000;
	public IConceptVector getConceptVectorUsingArray(String query) {
		if(tmpArray == null){
			tmpArray = new double[tmpArraySize];
		}else{
			Arrays.fill(tmpArray, 0);
		}
        getConceptVectorInternal(query, (id,s)->tmpArray[id]+= s);

        IConceptVector newCv = new TroveConceptVector(30);
        for(int i = 0; i < tmpArraySize; i++){
        	if(tmpArray[i] > 0.000001){
        		newCv.set(i, tmpArray[i]);
        	}
        }
        return newCv;
	}
	
	public HashMap<String,Integer> getBOW(String query, boolean stemming){
		HashMap<String,Integer> bow = new HashMap<>();
		String strTerm;
        TokenStream ts = analyzer.tokenStream("contents",new StringReader(query));
        try {
			ts.reset();
	        while (ts.incrementToken()) { 
	            TermAttribute t = ts.getAttribute(TermAttribute.class);
	            strTerm = t.term();
	            // records term counts for TF
	            Integer v = bow.get(strTerm);
	            v = (v == null ? 1 : v +1);
            	bow.put(strTerm, 1);
	        }
	                
	        ts.end();
	        ts.close();
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
        return bow;
	}
	
	/**
	 * Retrieves full vector for regular features
	 * @param query
	 * @return Returns concept vector results exist, otherwise null 
	 */
	public void getConceptVectorInternal(String query, BiConsumer<Integer,Double> consumer) {

		HashMap<String,Integer> bow = getBOW(query, true);
        if(bow.size() == 0){
        	return;
        }
       
        THashMap<String,byte[]> termVectors = TermQueryOptimizer.getInstance()
        		.doQuery(bow.keySet());

        int nTerms = bow.size();
        termVectors.forEach((k,v) -> {
        	try {
            	TermVectorIterator iter = new TermVectorIterator(v);
            	double termFreq = bow.get(k);
				while (iter.next()) {
					consumer.accept(iter.getConceptId(), iter.getConceptScore() * termFreq / nTerms);
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
        });
	}

	public IConceptVector getNormalVector(String query, int maxVectorLen) {
		IConceptVector cvBase = getConceptVector(query);
		
		if(cvBase == null){
			return null;
		}
		return getNormalVector(cvBase, maxVectorLen);
	}
	
	/**
	 * Returns trimmed form of concept vector
	 * @param cv
	 * @return
	 */
	public static IConceptVector getNormalVector(IConceptVector cv, int LIMIT){
		IConceptVector cv_normal = new TroveConceptVector( LIMIT);
		IConceptIterator it;
		
		if(cv == null)
			return null;
		
		if (cv.size() <= LIMIT) {
			return cv;
		}
		
		it = cv.bestKOrderedIterator(LIMIT);
		
		while(it.next()){
			cv_normal.set(it.getId(), it.getValue());
		}
		
		return cv_normal;
	}
	
	public IConceptVector getLinkVector(IConceptVector cv, int limit) {
		if(cv == null)
			return null;
		return getLinkVector(cv, LINK_ALPHA, limit);
	}
	
	Set<Integer> ids = new THashSet<Integer>();
	TIntDoubleHashMap bonus = new TIntDoubleHashMap();
	public IConceptVector getLinkVector(IConceptVector cv, double ALPHA, int LIMIT) {
		if(cv == null)
			return null;

		IConceptIterator it = cv.iterator();
		ids.clear();
		while(it.next()){
			ids.add(it.getId());
		}
		
		Map<Integer, byte[]> concept2nd = Concept2ndOrderQueryOptimizer.getInstance()
				.doQuery(ids);
		
		bonus.clear();
		
		it = cv.iterator();
		while(it.next()){
			int conceptId = it.getId();
			double conceptScore = it.getValue();
			if (!concept2nd.containsKey(conceptId)) {
				continue;
			}
			TermVectorIterator tvi = new TermVectorIterator(concept2nd.get(conceptId));
			while(tvi.next()) {
				int targetId = tvi.getConceptId();
				bonus.put(targetId, 
						tvi.getConceptScore() * conceptScore + bonus.get(targetId));
			}
		}

		IConceptVector linkVector = new TroveConceptVector(bonus.size());
		int[] index = bonus.keys();
		double[] values = bonus.getValues();
		
		// We want the best LIMIT concepts.
		// only if there are more then LIMIT concepts, use HeapSort to find the best ones
		if (LIMIT < index.length) {
			InplaceSorts.quicksort(values, index);
		}
		
		int c = 0;
		for(int i = index.length - 1; i >= 0; i--){
			linkVector.set(index[i], values[i] * ALPHA);
			c++;
			if(c >= LIMIT){
				break;
			}
		}
		
		return linkVector;
	}
	
	/**
	 * 
	 * @param limit				Max number of top scoring concepts to return
	 * @param secondOrderLimit	When doing 2nd order vector, max number of top
	 * 							bonus scoring elements to update
	 */
	public IConceptVector getCombinedVector(String query, int maxVectorLen) {
		IConceptVector cvBase = getConceptVector(query);
		return getCombinedVector(cvBase, maxVectorLen);
	}

	public IConceptVector getCombinedVector(IConceptVector cvBase, int maxVectorLen) {
		IConceptVector cvNormal, cvLink;
		
		if(cvBase == null){
			return null;
		}
		cvNormal = getNormalVector(cvBase, maxVectorLen);
		cvLink = getLinkVector(cvNormal,maxVectorLen);
		
		cvNormal.add(cvLink);
		
		return cvNormal;
	}
	
	public IConceptVector getConceptESAVector(byte[] vec) {
		TermVectorIterator tvi = new TermVectorIterator(vec);
		IConceptVector result = new TroveConceptVector(tvi.getVectorLen());
		while(tvi.next()) {
			result.set(tvi.getConceptId(), 
					tvi.getConceptScore());
		}
		return result;
	}
	
	public IConceptVector getConceptESAVector(int conceptId) {
		Map<Integer,byte[]> map = ConceptESAVectorQueryOptimizer.getInstance()
				.doQuery(new HashSet<Integer>(Arrays.asList(conceptId)));

		if (map.isEmpty()) {
			return null;
		}

		return getConceptESAVector(map.get(conceptId));
	}
	
	public double getRelatedness(IConceptVector v1,IConceptVector v2){
		return ConceptVectorCosineSimilarity.cosineSimilarity(v1,v2);
	}
}
