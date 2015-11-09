package edu.wiki.search;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.api.concept.scorer.CosineScorer;
import edu.wiki.concept.ConceptVectorSimilarity;
import edu.wiki.concept.TroveConceptVector;
import edu.wiki.index.WikipediaAnalyzer;
import edu.wiki.util.TermVectorIterator;
import edu.wiki.util.WikiprepESAConfiguration;
import edu.wiki.util.db.Concept2ndOrderQueryOptimizer;
import edu.wiki.util.db.IdfQueryOptimizer;
import edu.wiki.util.db.TermQueryOptimizer;

/**
 * Performs search on the index located in database.
 * 
 * @author Cagatay Calli <ccalli@gmail.com>
 */
public class ESASearcher {
	WikipediaAnalyzer analyzer;
	
	HashMap<String, Integer> freqMap = new HashMap<String, Integer>(30);
	HashMap<String, Double> tfidfMap = new HashMap<String, Double>(30);
	
	ArrayList<String> termList = new ArrayList<String>(30);
	
	static float LINK_ALPHA = 0.5f;
	
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
		String strTerm;
		int numTerms = 0;
		int doc;
		double score;
		int vint;
		double vdouble;
		double tf;
		double vsum;
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
       
        Map<Integer, Double> result = new HashMap<Integer, Double>();  

        score = 0;
        Map<String,byte[]> termVectors = TermQueryOptimizer.getInstance()
        		.doQuery(new HashSet<String>(termList));

        for (Entry<String, byte[]> entry : termVectors.entrySet()) { 
        	TermVectorIterator iter = new TermVectorIterator(entry.getValue());
        	while (iter.next()) {
				doc = iter.getConceptId();
				score = iter.getConceptScore() * tfidfMap.get(entry.getKey());
				Double curr = result.get(doc);
				if (curr != null) {
					score += curr;
				}
				result.put(doc, score);
        	}
    	}
        
        // no result
        if(score == 0){
        	return null;
        }

        
        IConceptVector newCv = new TroveConceptVector(result.size());
		for( Entry<Integer, Double> e : result.entrySet()) {
			newCv.set( e.getKey(), e.getValue() / numTerms );
		}
		
		return newCv;
	}
	
	public IConceptVector getNormalVector(String query) throws IOException {
		IConceptVector cvBase = getConceptVector(query);
		
		if(cvBase == null){
			return null;
		}
		return getNormalVector(cvBase, WikiprepESAConfiguration.getInstance().getIntProperty(WikiprepESAConfiguration.NORMALIZED_VECTOR_SIZE_LIMIT));
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
		
		it = cv.orderedIterator();
		
		int count = 0;
		while(it.next()){
			if(count >= LIMIT) break;
			cv_normal.set(it.getId(), it.getValue());
			count++;
		}
		
		return cv_normal;
	}
	
	public IConceptVector getLinkVector(IConceptVector cv, int limit) {
		if(cv == null)
			return null;
		return getLinkVector(cv, LINK_ALPHA, limit);
	}
	
	public IConceptVector getLinkVector(IConceptVector cv, double ALPHA, int LIMIT) {
		if(cv == null)
			return null;

		IConceptIterator it = cv.orderedIterator();
		Set<Integer> ids = new HashSet<Integer>();
		while(it.next()){
			ids.add(it.getId());
		}
		
		Map<Integer, byte[]> concept2nd = Concept2ndOrderQueryOptimizer.getInstance()
				.doQuery(ids);
		
		Map<Integer, Double> bonus = new HashMap<Integer, Double>();
		
		it = cv.orderedIterator();
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
							tvi.getConceptScore() * conceptScore + (bonus.get(targetId) == null ? 0.0 : bonus.get(targetId)));
				}
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}

		IConceptVector linkVector = new TroveConceptVector(bonus.size());
		List<Entry<Integer,Double>> sortedEntries = new ArrayList<Entry<Integer,Double>>(bonus.entrySet());
		Collections.sort(sortedEntries, (v1, v2) -> - Double.compare(v1.getValue(), v2.getValue()));
		
		int c = 0;
		for(Entry<Integer, Double> e : sortedEntries){
			linkVector.set(e.getKey(), e.getValue() * ALPHA);
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
	public IConceptVector getCombinedVector(String query) throws IOException {
		IConceptVector cvBase = getConceptVector(query);
		IConceptVector cvNormal, cvLink;
		
		if(cvBase == null){
			return null;
		}
		cvNormal = getNormalVector(cvBase, WikiprepESAConfiguration.getInstance().getIntProperty(WikiprepESAConfiguration.NORMALIZED_VECTOR_SIZE_LIMIT));
		cvLink = getLinkVector(cvNormal,WikiprepESAConfiguration.getInstance().getIntProperty(WikiprepESAConfiguration.SECOND_ORDER_BONUS_VECTOR_LIMIT));
		
		cvNormal.add(cvLink);
		
		return cvNormal;
	}
	
	/**
	 * Calculate semantic relatedness between documents
	 * @param doc1
	 * @param doc2
	 * @return returns relatedness if successful, -1 otherwise
	 */
	public double getRelatedness(String doc1, String doc2){
		try {
			// IConceptVector c1 = getCombinedVector(doc1);
			// IConceptVector c2 = getCombinedVector(doc2);
			// IConceptVector c1 = getNormalVector(getConceptVector(doc1),10);
			// IConceptVector c2 = getNormalVector(getConceptVector(doc2),10);
			
			IConceptVector c1 = getConceptVector(doc1);
			IConceptVector c2 = getConceptVector(doc2);
			
			if(c1 == null || c2 == null){
				// return 0;
				return -1;	// undefined
			}
			
			final double rel = sim.calcSimilarity(c1, c2);
			
			// mark for dealloc
			c1 = null;
			c2 = null;
			
			return rel;

		}
		catch(Exception e){
			e.printStackTrace();
			return 0;
		}

	}

}
