package edu.wiki.search;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.api.concept.scorer.CosineScorer;
import edu.wiki.concept.ConceptVectorSimilarity;
import edu.wiki.concept.TroveConceptVector;
import edu.wiki.index.WikipediaAnalyzer;
import edu.wiki.util.HeapSort;
import edu.wiki.util.WikiprepESAConfiguration;
import edu.wiki.util.WikiprepESAdb;
import edu.wiki.util.db.IdfQueryOptimizer;
import edu.wiki.util.db.TermQueryOptimizer;
import gnu.trove.TIntFloatHashMap;
import gnu.trove.TIntIntHashMap;

/**
 * Performs search on the index located in database.
 * 
 * @author Cagatay Calli <ccalli@gmail.com>
 */
public class ESASearcher {
	Connection connection;
	
	PreparedStatement pstmtLinks;
	Statement stmtInlink;
	
	WikipediaAnalyzer analyzer;
	
	String strMaxConcept = "SELECT MAX(id) FROM article";
	
	String strInlinks = "SELECT i.target_id, i.inlink FROM inlinks i WHERE i.target_id IN ";
	
	String strLinks = "SELECT target_id FROM pagelinks WHERE source_id = ?";

	int maxConceptId;
	
	int[] ids;
	double[] values;
	
	HashMap<String, Integer> freqMap = new HashMap<String, Integer>(30);
	HashMap<String, Double> tfidfMap = new HashMap<String, Double>(30);
	
	ArrayList<String> termList = new ArrayList<String>(30);
	
	TIntIntHashMap inlinkMap;
	
	static float LINK_ALPHA = 0.5f;
	
	ConceptVectorSimilarity sim = new ConceptVectorSimilarity(new CosineScorer());
		
	public void initDB() throws ClassNotFoundException, SQLException, IOException {
		connection = WikiprepESAdb.getInstance().getConnection();
		
		pstmtLinks = connection.prepareStatement(strLinks);
		pstmtLinks.setFetchSize(500);
		
		stmtInlink = connection.createStatement();
		stmtInlink.setFetchSize(50);
		
		ResultSet res = connection.createStatement().executeQuery(strMaxConcept);
		res.next();
		maxConceptId = res.getInt(1) + 1;
  }
	
	public void clean(){
		freqMap.clear();
		tfidfMap.clear();
		termList.clear();
		inlinkMap.clear();
		
		Arrays.fill(ids, 0);
		Arrays.fill(values, 0);
	}
	
	public ESASearcher() throws ClassNotFoundException, SQLException, IOException{
		initDB();
		analyzer = new WikipediaAnalyzer();
		
		ids = new int[maxConceptId];
		values = new double[maxConceptId];
		
		inlinkMap = new TIntIntHashMap(300);
	}
	
	@Override
	protected void finalize() throws Throwable {
        connection.close();
		super.finalize();
	}
	
	/**
	 * Retrieves full vector for regular features
	 * @param query
	 * @return Returns concept vector results exist, otherwise null 
	 * @throws IOException
	 * @throws SQLException
	 */
	public IConceptVector getConceptVector(String query) throws IOException, SQLException{
		String strTerm;
		int numTerms = 0;
		int doc;
		double score;
		int vint;
		double vdouble;
		double tf;
		double vsum;
		int plen;
        TokenStream ts = analyzer.tokenStream("contents",new StringReader(query));
        ByteArrayInputStream bais;
        DataInputStream dis;

        this.clean();

		for( int i=0; i<ids.length; i++ ) {
			ids[i] = i;
		}
        
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
        
        score = 0;
        Map<String,byte[]> termVectors = TermQueryOptimizer.getInstance()
        		.doQuery(new HashSet<String>(termList));
        for (Entry<String, byte[]> entry : termVectors.entrySet()) { 
        	            
        	bais = new ByteArrayInputStream(entry.getValue());
        	dis = new DataInputStream(bais);
  
        	/**
			 * 4 bytes: int - length of array
			 * 4 byte (doc) - 8 byte (tfidf) pairs
			 */
			  
			plen = dis.readInt();
			// System.out.println("vector len: " + plen);
			for(int k = 0;k<plen;k++){
				doc = dis.readInt();
				score = dis.readFloat();
				values[doc] += score * tfidfMap.get(entry.getKey());
			}
  
			bais.close();
			dis.close();
    	}
        
        // no result
        if(score == 0){
        	return null;
        }
        
        HeapSort.heapSort( values, ids );
        
        IConceptVector newCv = new TroveConceptVector(ids.length);
		for( int i=ids.length-1; i>=0 && values[i] > 0; i-- ) {
			newCv.set( ids[i], values[i] / numTerms );
		}
		
		return newCv;
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
	
	private TIntIntHashMap setInlinkCounts(Collection<Integer> ids) throws SQLException{
		inlinkMap.clear();
		
		String inPart = "(";
		
		for(int id: ids){
			inPart += id + ",";
		}
		
		inPart = inPart.substring(0,inPart.length()-1) + ")";

		// collect inlink counts
		ResultSet r = stmtInlink.executeQuery(strInlinks + inPart);
		while(r.next()){
			inlinkMap.put(r.getInt(1), r.getInt(2)); 
		}
		
		return inlinkMap;
	}
	
	private Collection<Integer> getLinks(int id) throws SQLException{
		ArrayList<Integer> links = new ArrayList<Integer>(100); 
		
		pstmtLinks.setInt(1, id);
		
		ResultSet r = pstmtLinks.executeQuery();
		while(r.next()){
			links.add(r.getInt(1)); 
		}
		
		return links;
	}
	
	
	public IConceptVector getLinkVector(IConceptVector cv, int limit) throws SQLException {
		if(cv == null)
			return null;
		return getLinkVector(cv, true, LINK_ALPHA, limit);
	}
	
	/**
	 * Computes secondary interpretation vector of regular features
	 * @param cv
	 * @param moreGeneral
	 * @param ALPHA
	 * @param LIMIT
	 * @return
	 * @throws SQLException
	 */
	public IConceptVector getLinkVector(IConceptVector cv, boolean moreGeneral, double ALPHA, int LIMIT) throws SQLException {
		IConceptIterator it;
		
		if(cv == null)
			return null;
		
		it = cv.orderedIterator();
		
		ArrayList<Integer> pages = new ArrayList<Integer>();
						
		TIntFloatHashMap valueMap2 = new TIntFloatHashMap(1000);
		TIntFloatHashMap valueMap3 = new TIntFloatHashMap();
		
		ArrayList<Integer> npages = new ArrayList<Integer>();
		
		HashMap<Integer, Float> secondMap = new HashMap<Integer, Float>(1000);
		
		
		this.clean();
				
		// collect article objects
		while(it.next()){
			pages.add(it.getId());
			valueMap2.put(it.getId(),(float) it.getValue());
		}
		
		// prepare inlink counts
		setInlinkCounts(pages);
				
		for(int pid : pages){			
			Collection<Integer> raw_links = getLinks(pid);
			if(raw_links.isEmpty()){
				continue;
			}
			ArrayList<Integer> links = new ArrayList<Integer>(raw_links.size());
			
			final double inlink_factor_p = Math.log(inlinkMap.get(pid));
										
			float origValue = valueMap2.get(pid);
			
			setInlinkCounts(raw_links);
						
			for(int lid : raw_links){
				final double inlink_factor_link = Math.log(inlinkMap.get(lid));
				
				// check concept generality..
				if(inlink_factor_link - inlink_factor_p > 1){
					links.add(lid);
				}
			}
						
			for(int lid : links){				
				if(!valueMap2.containsKey(lid)){
					valueMap2.put(lid, 0.0f);
					npages.add(lid);
				}
			}
						
			
			
			float linkedValue = 0.0f;
									
			for(int lid : links){
				if(valueMap3.containsKey(lid)){
					linkedValue = valueMap3.get(lid); 
					linkedValue += origValue;
					valueMap3.put(lid, linkedValue);
				}
				else {
					valueMap3.put(lid, origValue);
				}
			}
			
		}
		
		
//		for(int pid : pages){			
//			if(valueMap3.containsKey(pid)){
//				secondMap.put(pid, (float) (valueMap2.get(pid) + ALPHA * valueMap3.get(pid)));
//			}
//			else {
//				secondMap.put(pid, (float) (valueMap2.get(pid) ));
//			}
//		}
		
		for(int pid : npages){			
			secondMap.put(pid, (float) (ALPHA * valueMap3.get(pid)));

		}
		
		
		//System.out.println("read links..");
		
		
		ArrayList<Integer> keys = new ArrayList<Integer>(secondMap.keySet());
		
		//Sort keys by values.
		final Map<Integer, Float> langForComp = secondMap;
		Collections.sort(keys, 
			new Comparator<Integer>(){
				public int compare(Integer left, Integer right){
					Float leftValue = (Float)langForComp.get(left);
					Float rightValue = (Float)langForComp.get(right);
					return leftValue.compareTo(rightValue);
				}
			});
		Collections.reverse(keys);
		
		

		IConceptVector cv_link = new TroveConceptVector(maxConceptId);
		
		int c = 0;
		for(int p : keys){
			cv_link.set(p, secondMap.get(p));
			c++;
			if(c >= LIMIT){
				break;
			}
		}
		
		
		return cv_link;
	}
	
	/**
	 * 
	 * @param limit				Max number of top scoring concepts to return
	 * @param secondOrderLimit	When doing 2nd order vector, max number of top
	 * 							bonus scoring elements to update
	 */
	public IConceptVector getCombinedVector(String query) throws IOException, SQLException{
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
