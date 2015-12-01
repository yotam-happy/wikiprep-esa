package edu.wiki.search;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import edu.wiki.api.concept.IConceptVector;
import edu.wiki.util.db.ArticleQueryOptimizer;
import edu.wiki.util.db.ConceptESAVectorQueryOptimizer;

public class DisambiguatingText2Features {
	public static final int CONTEXT_SIZE = 10;
	public static final int MAX_NGRAM = 3;
	public static final double MATCH_CUTOFF = 0.01;
	ESASearcher searcher;
	ArticleQueryOptimizer articleQueryOptimizer;
	ConceptESAVectorQueryOptimizer conceptESAVectorQueryOptimizer;
	
	public DisambiguatingText2Features() throws IOException, ClassNotFoundException {
		searcher = new ESASearcher();
		articleQueryOptimizer = ArticleQueryOptimizer.getInstance();
		conceptESAVectorQueryOptimizer = ConceptESAVectorQueryOptimizer.getInstance();
	}
	
	public Map<String, Double> getFeatures(String doc) {
		Map<String, Double> result = new HashMap<>();
		
		return result;
	}
	
	public Map<Integer, Double> getDisambiguatingFeatures(String doc) throws IOException {
		Map<Integer, Double> result = new HashMap<>();
		ArrayList<String> tokens = new ArrayList<>();

		// Tokenize
		TokenStream ts = searcher.analyzer.tokenStream("contents",new StringReader(doc));
        try {
			while (ts.incrementToken()) { 
	            TermAttribute t = ts.getAttribute(TermAttribute.class);
	            tokens.add(t.term());
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

        if (tokens.size() < CONTEXT_SIZE / 2) {
        	// Can't reliably disambiguate anything
        	return result;
        }
        // Disambiguate starting on each word
        for(int i = 0; i < tokens.size(); i++) {
        	// bias to context to the front of the inspected ngram
        	int contextStart = i - CONTEXT_SIZE / 4 < 0 ? 0 : i - CONTEXT_SIZE / 4;
        	int contextEnd = contextStart + CONTEXT_SIZE > tokens.size() ? tokens.size() : contextStart + CONTEXT_SIZE;
        	if (contextEnd < CONTEXT_SIZE) {
        		contextStart = 0;
        	}
        	StringBuffer context = new StringBuffer();
        	for (int j = contextStart; j < contextEnd; j++){
        		if (j != i) {
        			context.append(tokens.get(j)).append(' ');
        		}
        	}
        	// Get word to context similarity (should use getCombinedVector but
        	// having performance issues
        	IConceptVector contextVec = searcher.getConceptVector(context.toString());
        	
        	// find possible concepts for disambiguation
        	Set<String> candidates = new HashSet<>();
        	StringBuffer sbCandidate = new StringBuffer();
        	sbCandidate.append("$");
        	for(int j = i; j < i + MAX_NGRAM && j < tokens.size(); j ++) {
        		sbCandidate.append('_');
        		sbCandidate.append(tokens.get(j));
        		candidates.add(sbCandidate.toString());
        	}
        	Map<String, Set<Integer>> candidatesIds = articleQueryOptimizer.doQuery(candidates);
        	
        	// Get candidates
        	Map<Integer, Double> candidatesScores = new HashMap<>();
        	candidatesIds.forEach((name,ids) -> {
        		ids.forEach((id -> {
            		try {
    					IConceptVector vec = searcher.getConceptESAVector(id);
    					candidatesScores.put(id, searcher.getRelatedness(contextVec, vec));
    				} catch (Exception e) {
    					throw new RuntimeException(e);
    				}
        		}));
        	});
        	
        	// choose best candidate
        	Entry<Integer, Double> best = candidatesScores.entrySet().stream()
        			.max((e1,e2)->e1.getValue().compareTo(e2.getValue())).orElse(null);
        	if (best != null && best.getValue() > MATCH_CUTOFF) {
        		result.put(best.getKey(), 
        				1 + (result.containsKey(best.getKey()) ? result.get(best.getKey()) : 0) );
        	}
        }
        
		return result;
	}
}
