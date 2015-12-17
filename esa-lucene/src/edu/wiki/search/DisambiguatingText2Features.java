package edu.wiki.search;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import edu.wiki.api.concept.IConceptVector;
import edu.wiki.util.db.SurfaceNamesQueryOptimizer;
import edu.wiki.util.db.ConceptESAVectorQueryOptimizer;

public class DisambiguatingText2Features {
	public static final int MIN_CONTEXT_SZ = 15;
	public static final int MAX_CONTEXT_CONCEPTS_TO_KEEP = 1000;
	public static final int MAX_NGRAM = 2; // Low MAX_NGRAM can match only articles with short titles. These can be
											// assumed the more general concepts and therefore the more important ones
											// so the accuracy loss might be worth the speed gain.
	public static final double MATCH_CUTOFF = 0.01;
	ESASearcher searcher;
	SurfaceNamesQueryOptimizer articleQueryOptimizer;
	ConceptESAVectorQueryOptimizer conceptESAVectorQueryOptimizer;
	
	public DisambiguatingText2Features() throws IOException, ClassNotFoundException {
		searcher = new ESASearcher();
		articleQueryOptimizer = SurfaceNamesQueryOptimizer.getInstance();
		conceptESAVectorQueryOptimizer = ConceptESAVectorQueryOptimizer.getInstance();
	}
	
	public Map<String, Double> getFeatures(String doc) {
		Map<String, Double> result = new HashMap<>();
		
		return result;
	}
	
	static int c = 0;
	static int d = 0;
	static int g = 0;
	public static synchronized void dod() {
		if(c % 500 == 0) {
			System.out.println("disambiguation features per article: " + ((double)d / c) + " relative: " + ((double)d / g));
		}
	}

	static int k, kk, kkk;
	public Map<Integer, Double> getDisambiguatingFeatures(Stream<String> contexts, int overlap) throws IOException {
		Map<Integer, Double> result = new HashMap<>();
		c++;
		k = 0;
		kk = 0;
		kkk = 0;
		contexts.forEach((context) -> {
			k++;
			ArrayList<String> tokens = new ArrayList<>();
			// Tokenize
			TokenStream ts = searcher.analyzer.tokenStream("contents",new StringReader(context));
	        try {
				while (ts.incrementToken()) { 
					g++;
		            TermAttribute t = ts.getAttribute(TermAttribute.class);
		            tokens.add(t.term());
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

	        if (tokens.size() < MIN_CONTEXT_SZ) {
	        	// can't reliably disambiguate anything
	        	return;
	        }
			
        	IConceptVector contextVec;
			try {
				contextVec = searcher.getNormalVector(searcher.getConceptVector(context), MAX_CONTEXT_CONCEPTS_TO_KEEP);
				
			} catch (Exception e3) {
				throw new RuntimeException(e3);
			}

			if (contextVec == null) {
				return;
			}
			// Disambiguate starting on each word
	        for(int i = 0; i < tokens.size() - overlap; i++) {
	        	if (i < overlap) {
	        		continue;
	        	}
	        	// find possible candidates for disambiguation
	        	Set<String> candidates = new HashSet<>();
	        	StringBuffer sbCandidate = new StringBuffer();
	        	sbCandidate.append("$");
	        	for(int j = i; j < i + MAX_NGRAM && j < tokens.size(); j ++) {
	        		sbCandidate.append('_');
	        		sbCandidate.append(tokens.get(j));
	        		candidates.add(sbCandidate.toString());
	        	}
	        	kk+=candidates.size();
	        	Map<String, Set<Integer>> candidatesIds = articleQueryOptimizer.doQuery(candidates);
	        	kkk+=candidatesIds.size();

	        	if (candidatesIds.isEmpty()) {
	        		continue;
	        	}
	        	Set<Integer> s = candidatesIds.values().stream().
	        			flatMap((c)-> c.stream()).
	        			collect(Collectors.toSet());
	        	Map<Integer, byte[]> candidateVectors = ConceptESAVectorQueryOptimizer.getInstance().doQuery(
	        			s);
	        	// Get candidate scores
	        	Map<Integer, Double> candidatesScores = new HashMap<>();
	        	candidatesIds.forEach((name,ids) -> {
	        		ids.forEach((id -> {
	            		try {
	            			if (candidateVectors.get(id) != null){
		    					IConceptVector vec = searcher.getConceptESAVector(candidateVectors.get(id));
		    					candidatesScores.put(id, searcher.getRelatedness(contextVec, vec));
	            			}
	    				} catch (Exception e) {
	    					throw new RuntimeException(e);
	    				}
	        		}));
	        	});
	        	
	        	// best candidate score
	        	Entry<Integer, Double> best = candidatesScores.entrySet().stream()
	        			.max((e1,e2)->e1.getValue().compareTo(e2.getValue())).orElse(null);
	        	
	        	if (best != null && best.getValue() > MATCH_CUTOFF) {
	        		result.put(best.getKey(), 
	        				1 + (result.containsKey(best.getKey()) ? result.get(best.getKey()) : 0) );
					d++;

					// another option:
/*					candidatesScores.entrySet().stream().filter((e)->e.getValue() > best.getValue() / 2)
	        				.forEach((e)->{
	        	        		result.put(e.getKey(), 
	        	        				1 + (result.containsKey(e.getKey()) ? result.get(e.getKey()) : 0) );
	        					d++;
	        				});*/	
	        	}
	        }
		});
		dod();
		return result;
	}
}
