package edu.wiki.search;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.concept.ConceptVectorCosineSimilarity;
import edu.wiki.concept.TroveConceptVector;
import edu.wiki.index.WikipediaAnalyzer;
import edu.wiki.util.InplaceSorts;
import edu.wiki.util.TermVectorIterator;
import edu.wiki.util.db.ArticleLengthQueryOptimizer;
import edu.wiki.util.db.Concept2ndOrderQueryOptimizer;
import edu.wiki.util.db.ConceptESAVectorQueryOptimizer;
import edu.wiki.util.db.TermQueryOptimizer;
import gnu.trove.THashMap;
import gnu.trove.THashSet;
import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntDoubleIterator;
import gnu.trove.TObjectIntHashMap;

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
	
	/**
	 * Retrieves full vector for regular features
	 * @param query
	 * @return Returns concept vector results exist, otherwise null 
	 */
	public IConceptVector getConceptVector(String query) {
		int numTerms = 0;
		TObjectIntHashMap<String> freqMap = new TObjectIntHashMap<String>();
//		TObjectDoubleHashMap<String> tfidfMap = new TObjectDoubleHashMap<String>();
		
		ArrayList<String> termList = new ArrayList<String>(30);
//		double vdouble;
//		double tf;
//		double vsum;
		int vint;
		numTerms = 0;
		String strTerm;
        TokenStream ts = analyzer.tokenStream("contents",new StringReader(query));
        try {
			ts.reset();
	        while (ts.incrementToken()) { 
	            TermAttribute t = ts.getAttribute(TermAttribute.class);
	            strTerm = t.term();
	            // records term counts for TF
	            if(freqMap.containsKey(strTerm)){
	            	vint = freqMap.get(strTerm);
	            	freqMap.put(strTerm, vint+1);
	            }
	            else {
	            	freqMap.put(strTerm, 1);
	            }
	            termList.add(strTerm);
	            numTerms++;	
	        }
	                
	        ts.end();
	        ts.close();
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
        
        if(numTerms == 0){
        	return null;
        }

//    	Map<String, Float> idfMap = IdfQueryOptimizer.getInstance()
//    			.doQuery(new HashSet<String>(termList));

        // calculate TF-IDF vector (normalized)
//        vsum = 0;
//        for(String tk : idfMap.keySet()){
//        	tf = 1.0 + Math.log(freqMap.get(tk));
//        	vdouble = (idfMap.get(tk) * tf);
//        	tfidfMap.put(tk, vdouble);
//        	vsum += vdouble * vdouble;
//        }
//        vsum = Math.sqrt(vsum);
        
        
        // comment this out for canceling query normalization
        //for(String tk : idfMap.keySet()){
        //	vdouble = tfidfMap.get(tk);
        //	tfidfMap.put(tk, vdouble / vsum);
        //}
       
        TIntDoubleHashMap result = new TIntDoubleHashMap();  

        THashMap<String,byte[]> termVectors = TermQueryOptimizer.getInstance()
        		.doQuery(new HashSet<String>(termList));

        termVectors.forEach((k,v) -> {
        	try {
            	TermVectorIterator iter = new TermVectorIterator(v);
            	double termTfidf = freqMap.get(k);
				while (iter.next()) {
					int doc = iter.getConceptId();
					double score = iter.getConceptScore() * termTfidf + result.get(doc);
					if (score != 0) {
						result.put(iter.getConceptId(), score);
					}
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
        });
        
        // no result
        if(result.size() == 0){
        	return null;
        }

        
        IConceptVector newCv = new TroveConceptVector(result.size());
        TIntDoubleIterator iter = result.iterator();
        while(iter.hasNext()) {
        	iter.advance();
			newCv.set( iter.key(), iter.value() / numTerms );
        }
		return newCv;
	}

	private double linearTransform(double x, double domainLow, double domainHigh){
		return x < domainLow ? 0 : 
			(x > domainHigh ? 1 : 
				(x - domainLow) / (domainHigh - domainLow));
	}
	private double linearTransform(double x, 
			double domainLow, double domainHigh,
			double targetHigh, double targetLow){
		return linearTransform(x, domainLow, domainHigh) * (targetHigh - targetLow) + targetLow;
	}
	
	/**
	 * Retrieves full vector for regular features where we give a bonus
	 * to a concept if more then one word scores it.
	 * The bonus is dependant on semantic distance of the words that scored
	 * a concept where the words are less semantically related gets a bigger bonus 
	 * 
	 * @param query
	 * @return Returns concept vector results exist, otherwise null 
	 */
	public IConceptVector getConceptVector2(String query) {
		int numTerms = 0;
		TObjectIntHashMap<String> freqMap = new TObjectIntHashMap<String>();
		
		ArrayList<String> termList = new ArrayList<String>(30);

		int vint;
		numTerms = 0;
		String strTerm;
        TokenStream ts = analyzer.tokenStream("contents",new StringReader(query));
        try {
			ts.reset();
	        while (ts.incrementToken()) { 
	            TermAttribute t = ts.getAttribute(TermAttribute.class);
	            strTerm = t.term();
	            // records term counts for TF
	            if(freqMap.containsKey(strTerm)){
	            	vint = freqMap.get(strTerm);
	            	freqMap.put(strTerm, vint+1);
	            }
	            else {
	            	freqMap.put(strTerm, 1);
	            }
	            termList.add(strTerm);
	            numTerms++;	
	        }
	                
	        ts.end();
	        ts.close();
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
        
        if(numTerms == 0){
        	return null;
        }

		Map<Integer, Map<String,Double>> conceptTerm = new HashMap<>();

       
        TIntDoubleHashMap result = new TIntDoubleHashMap();  

        THashMap<String,byte[]> termVectors = TermQueryOptimizer.getInstance()
        		.doQuery(new HashSet<String>(termList));
        
        termVectors.forEach((k,v) -> {
        	try {
            	TermVectorIterator iter = new TermVectorIterator(v);
            	double termTfidf = freqMap.get(k);
				while (iter.next()) {
					int doc = iter.getConceptId();
					double score = iter.getConceptScore() * termTfidf + result.get(doc);
					if (score != 0) {
						result.put(iter.getConceptId(), score);
					}

					if(!conceptTerm.containsKey(doc)){
						conceptTerm.put(doc, new HashMap<>());
					}
					conceptTerm.get(doc).put(k, iter.getConceptScore() * termTfidf);
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
        });

        TIntDoubleIterator it = result.iterator();
        while(it.hasNext()){
        	it.advance();
        	int concept = it.key();
        	if(!conceptTerm.containsKey(concept)){
        		continue;
        	}
        	
        	// accumulates distances between terms,
        	// we assume that far away terms mean the terms cover the document better
/*        	double termCoverage = 0;
    		Map<String,Double> tl = new HashMap<>(conceptTerm.get(concept));
    		double sampleRate = tl.size() < 7 ? 1.0 : 30 / (tl.size() * (tl.size() - 1)) ;
        	for(Entry<String,Double> t1 : conceptTerm.get(concept).entrySet()){
            	for(Entry<String,Double> t2 : tl.entrySet()){
            		
            		if(t1.getKey().compareTo(t2.getKey()) <= 0){
            			continue;
            		}
            		if (Math.random() > sampleRate){
            			continue;
            		}
            		termCoverage += getTermSimilarity(t1.getKey(),t2.getKey());
            	}
        	}*/
        	double termCoverage = conceptTerm.get(concept).size();
        	
        	
        	final double goodCoverageThreshhold = 2;
        	final double badCoverageThreshhold = 1;
        	final double trustedArticleLengthThreshhold = 1000;
        	final double untrustedArticleLenghThreshhold = 200;
        	final double maxTrustFactor = 1;
        	final double NoTrustFactor = 0.3;
        	
        	double coverage = linearTransform(termCoverage, badCoverageThreshhold, goodCoverageThreshhold); 
        			
        	// get article lengths
        	double articleLen = ArticleLengthQueryOptimizer.getInstance().doQuery(concept);
        	
        	double trust = linearTransform(articleLen, 
        			untrustedArticleLenghThreshhold, 
        			trustedArticleLengthThreshhold); 
        	
        	// we penalize short articles unless they have high termCoverage.
        	// the idea is that we require more evidence for short articles before
        	// we give them high scores
        	double trustPenaltyFactor = (1-trust)*(1-coverage);
        	double trustPenalty = linearTransform(trustPenaltyFactor, 0, 1, NoTrustFactor, maxTrustFactor); 
        	
        	// we give a bonus to concepts with high coverage.
        	double coverageBonus = termCoverage;
        	
        	it.setValue(it.value() * (1 + coverageBonus) * trustPenalty);
        }
        
        // no result
        if(result.size() == 0){
        	return null;
        }

        
        IConceptVector newCv = new TroveConceptVector(result.size());
        TIntDoubleIterator iter = result.iterator();
        while(iter.hasNext()) {
        	iter.advance();
			newCv.set( iter.key(), iter.value() / numTerms );
        }
		return newCv;
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

	public IConceptVector getCombinedVector2(String query, int maxVectorLen) {
		IConceptVector cvBase = getConceptVector2(query);
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
