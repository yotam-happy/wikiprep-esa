package edu.wiki.clustering.chinesewhisper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.search.ESASearcher;
import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntDoubleIterator;
import gnu.trove.TIntIntHashMap;

/**
 * Implementation of algorithm from:
 *
 * Biemann, Chris. "Chinese whispers: an efficient graph clustering algorithm and its 
 * application to natural language processing problems." Proceedings of the first workshop 
 * on graph based methods for natural language processing. Association for Computational 
 * Linguistics, 2006. * 
 *
 */
public class ChineseWhisper {
	Map<Integer,IConceptVector> vectors;
	TIntIntHashMap classes = new TIntIntHashMap();
	public ChineseWhisper(Map<Integer,IConceptVector> vectors) {
		this.vectors = vectors;
		for(Integer i : vectors.keySet()){
			classes.put(i, i);
		}
	}
	
	public Map<Integer,List<Integer>> run(int nIterations){
		int k = 0;
		System.out.println("Starting Chinese Whisper algorithm");
		
		while (k < nIterations){
			System.out.println("Doing iteration " + (k+1));
			classes = doStep();

			int treshhold = 10;
			Map<Integer,List<Integer>> ttt = inversClasses();
			int nSingleVertexClusters = ttt.values().stream().map((l)->l.size() < treshhold ? 1 : 0).
					collect(Collectors.summingInt(i->i));
			Object[] ss = ttt.values().stream()
					.map((l)->(Integer)l.size()).filter((i) -> i >= treshhold)
					.sorted().toArray();
			int median = (Integer)ss[ss.length / 2];
			int median34 = (Integer)ss[ss.length * 3 / 4];
			int median78 = (Integer)ss[ss.length * 7 / 8];
			System.out.println("Classes: " + ttt.size() + 
					" sz>="+treshhold+": " + (ttt.size() - nSingleVertexClusters) +
					" Median " + median +
					" Median3/4 " + median34 +
					" Median7/8 " + median78);
			k++;
		}
		System.out.println("Done Chinese Whisper algorithm");
		return inversClasses();
	}
	static int stepChanges;
	static int c;
	private TIntIntHashMap doStep() {
		ESASearcher esa;
		try {
			esa = new ESASearcher();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		TIntIntHashMap tmp = new TIntIntHashMap();
		stepChanges = 0;
		c = 0;
		vectors.forEach((id, vec) -> {
			if (c % (vectors.size() / 100) == 0){
				System.out.println("done " + (c / (vectors.size() / 100)) + "%");
			}
			c++;
			TIntDoubleHashMap classCandidates = new TIntDoubleHashMap();
			
			// calculate score of each class
			IConceptIterator it = vec.iterator();
			while(it.next()){
				// TODO: need to implement edge wiegth here. Should try a few alternatives
				// Note the most simplistic definition is to use it.score(). This assumes
				// a directed graphs and defines my class as the class of the concept most
				// describing me.
				// I rather use min(it.score(),vectors.get(it.id()).get(id)).
				// These clusters are meant to be focused around mutually describing concepts.
				
				double score = esa.getRelatedness(vectors.get(it.getId()), vec);
//				double score = Math.min(it.getValue(), vectors.get(it.getId()).get(id));
				int cls = classes.get(it.getId());
				classCandidates.put(cls, classCandidates.get(cls) + score);
			}
			
			// get class with highest score
			TIntDoubleIterator it2 = classCandidates.iterator();
			int cls = 0;
			double max = Double.MIN_VALUE;
			while(it2.hasNext()){
				it2.advance();
				if (it2.value() > max){
					cls = it2.key();
					max = it2.value();
				}
			}
			
			// assign correct class
			if (classes.get(id) != cls) {
				stepChanges ++;
			}
			tmp.put(id, cls);
		});
		System.out.println("changes " + stepChanges);
		return tmp;
	}
	
	private Map<Integer,List<Integer>> inversClasses() {
		Map<Integer,List<Integer>> ret = new HashMap<>();
		classes.forEachEntry((id, cls)->{
			List<Integer> l = ret.get(cls);
			if (l == null) {
				l = new ArrayList<>();
				ret.put(cls, l);	
			}
			l.add(id);
			return true;
		});
		return ret;
	}
}
