package edu.wiki.search;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.api.concept.scorer.CosineScorer;
import edu.wiki.concept.ConceptVectorSimilarity;
import edu.wiki.concept.TroveConceptVector;
import edu.wiki.index.WikipediaAnalyzer;
import edu.wiki.util.InplaceSorts;
import edu.wiki.util.TermVectorIterator;
import edu.wiki.util.db.Concept2ndOrderQueryOptimizer;
import edu.wiki.util.db.ConceptESAVectorQueryOptimizer;
import edu.wiki.util.db.IdfQueryOptimizer;
import edu.wiki.util.db.TermQueryOptimizer;
import gnu.trove.THashMap;
import gnu.trove.THashSet;
import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntDoubleIterator;
import gnu.trove.TObjectDoubleHashMap;
import gnu.trove.TObjectIntHashMap;

/**
 * Performs search on the index located in database.
 * 
 * @author Cagatay Calli <ccalli@gmail.com>
 */
public class ESASearcher {
	WikipediaAnalyzer analyzer;
	
	int numTerms = 0;
	TObjectIntHashMap<String> freqMap = new TObjectIntHashMap<String>();
	TObjectDoubleHashMap<String> tfidfMap = new TObjectDoubleHashMap<String>();
	
	ArrayList<String> termList = new ArrayList<String>(30);
	
	static final float LINK_ALPHA = 0.5f;
	
	ConceptVectorSimilarity sim = new ConceptVectorSimilarity(new CosineScorer());
		
	public void clean(){
		freqMap.clear();
		tfidfMap.clear();
		termList.clear();
	}
	
	public ESASearcher() throws ClassNotFoundException, IOException{
		analyzer = new WikipediaAnalyzer();
	}
	
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
	}
	
	/**
	 * Retrieves full vector for regular features
	 * @param query
	 * @return Returns concept vector results exist, otherwise null 
	 * @throws IOException
	 * @throws SQLException
	 */
	public IConceptVector getConceptVector(String query) throws IOException{
		double vdouble;
		double tf;
		double vsum;
		int vint;
		numTerms = 0;
		String strTerm;
        TokenStream ts = analyzer.tokenStream("contents",new StringReader(query));
        this.clean();
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
        
        if(numTerms == 0){
        	return null;
        }

    	Map<String, Float> idfMap = IdfQueryOptimizer.getInstance()
    			.doQuery(new HashSet<String>(termList));

        // calculate TF-IDF vector (normalized)
        vsum = 0;
        for(String tk : idfMap.keySet()){
        	tf = 1.0 + Math.log(freqMap.get(tk));
        	vdouble = (idfMap.get(tk) * tf);
        	tfidfMap.put(tk, vdouble);
        	vsum += vdouble * vdouble;
        }
        vsum = Math.sqrt(vsum);
        
        
        // comment this out for canceling query normalization
        for(String tk : idfMap.keySet()){
        	vdouble = tfidfMap.get(tk);
        	tfidfMap.put(tk, vdouble / vsum);
        }
       
        TIntDoubleHashMap result = new TIntDoubleHashMap();  

        THashMap<String,byte[]> termVectors = TermQueryOptimizer.getInstance()
        		.doQuery(new HashSet<String>(termList));

        termVectors.forEach((k,v) -> {
        	try {
            	TermVectorIterator iter = new TermVectorIterator(v);
            	double termTfidf = tfidfMap.get(k);
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
	
	public IConceptVector getNormalVector(String query, int maxVectorLen) throws IOException {
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
	public IConceptVector getNormalVector(IConceptVector cv, int LIMIT){
		IConceptVector cv_normal = new TroveConceptVector( LIMIT);
		IConceptIterator it;
		
		if(cv == null)
			return null;
		
		if (cv.count() <= LIMIT) {
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
			try {
				TermVectorIterator tvi = new TermVectorIterator(concept2nd.get(conceptId));
				while(tvi.next()) {
					int targetId = tvi.getConceptId();
					bonus.put(targetId, 
							tvi.getConceptScore() * conceptScore + bonus.get(targetId));
				}
			} catch(IOException e) {
				throw new RuntimeException(e);
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
	public IConceptVector getCombinedVector(String query, int maxVectorLen) throws IOException {
		IConceptVector cvBase = getConceptVector(query);
		IConceptVector cvNormal, cvLink;
		
		if(cvBase == null){
			return null;
		}
		cvNormal = getNormalVector(cvBase, maxVectorLen);
		cvLink = getLinkVector(cvNormal,maxVectorLen);
		
		cvNormal.add(cvLink);
		
		return cvNormal;
	}
	
	public IConceptVector getConceptESAVector(int conceptId) throws IOException {
		Map<Integer,byte[]> map = ConceptESAVectorQueryOptimizer.getInstance()
				.doQuery(new HashSet<Integer>(Arrays.asList(conceptId)));

		if (map.isEmpty()) {
			return null;
		}

		TermVectorIterator tvi = new TermVectorIterator(map.get(conceptId));
		IConceptVector result = new TroveConceptVector(tvi.getVectorLen());
		while(tvi.next()) {
			result.set(tvi.getConceptId(), 
					tvi.getConceptScore());
		}
		return result;
	}
	
	public double getRelatedness(IConceptVector v1, IConceptVector v2){
		if(v1 == null || v2 == null){
			return -1;
		}
		return sim.calcSimilarity(v1, v2);
	}
	/**
	 * Calculate semantic relatedness between documents
	 * @param doc1
	 * @param doc2
	 * @return returns relatedness if successful, -1 otherwise
	 */
	public double getRelatedness(String doc1, String doc2){
		try {
			IConceptVector c1 = getConceptVector(doc1);
			IConceptVector c2 = getConceptVector(doc2);
			return getRelatedness(c1, c2);
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}

	}

}
