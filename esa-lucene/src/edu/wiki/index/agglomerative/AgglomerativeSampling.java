package edu.wiki.index.agglomerative;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Does agglomerative clustering with sampling using some
 * 
 * @author yotamesh
 *
 */
public class AgglomerativeSampling<T> {
	Function<T,Double> evaluator = null;
	BiFunction<T,T,T> combiner = null;
	Map<T,Node<T>> lookup = new HashMap<>();
	List<Node<T>> nodes = new ArrayList<>();
	List<Double> probs = new ArrayList<>();
	Random rnd = new Random();
	int maxSamples = -1;

	public AgglomerativeSampling(
			Function<T,Double> evaluator, 
			BiFunction<T,T,T> combiner, 
			Collection<T> points){
		this.evaluator = evaluator;
		this.combiner = combiner;
		
		points.forEach((p)->{
			double v = evaluator.apply(p);
			Node<T> node = new Node<>(p, v);
			nodes.add(node);
			lookup.put(p, node);
		});
	}
	
	public void setMaxSamples(int maxSamples){
		this.maxSamples = maxSamples;
	}
	
	public void doClustering(int nClusters){
		int step = 0;
		System.out.println("AgglomerativeSampling doing clustering");
		int roundsNada = 0;
		while(nodes.size() > nClusters){
			updateProbs();
			Node<T> bestNode1 = null, bestNode2 = null;
			double bestEval = Double.NEGATIVE_INFINITY;
			double bestEvalDiff = Double.NEGATIVE_INFINITY;
			int samples = 0;
			do{
				Node<T> n1;
				Node<T> n2;
				if(nodes.size() < Math.sqrt(maxSamples)){
					if (samples / nodes.size() == samples % nodes.size()){
						samples++;
						continue;
					}
					n1 = nodes.get(samples / nodes.size());
					n2 = nodes.get(samples % nodes.size());
				} else {
					n1 = getRandomNode();
					n2 = getRandomNode();
				}
				
				double v = evaluator.apply(combiner.apply(n1.getPoint(),n2.getPoint()));
				double vDiff = v - ((n1.getEval() > n2.getEval()) ? 
						n1.getEval() : 
						n2.getEval());
				if(vDiff > bestEvalDiff){
					bestNode1 = n1;
					bestNode2 = n2;
					bestEval = v;
					bestEvalDiff = vDiff;
				}
				samples++;
			}while(samples < maxSamples && Math.sqrt(samples) < nodes.size());

			// couldn't find any expected improvement
			if(bestEvalDiff <= 0){
				roundsNada++;
				System.out.print(".");
				if(roundsNada >= 100){
					break;
				}
				continue;
			}
			roundsNada=0;
			// terribly inefficient
			nodes.remove(bestNode1);
			nodes.remove(bestNode2);
			
			Node<T> newNode = new Node<>(bestNode1, bestNode2, 
					combiner.apply(bestNode1.getPoint(), bestNode2.getPoint()),
					bestEval);
			nodes.add(newNode);
			
			step++;
			System.out.println("Done step " + step + " gain: " + bestEvalDiff + " new node size: " + newNode.subTreeSize);
		}
	}
	
	protected Node<T> getRandomNode(){
		//Double d = rnd.nextDouble();
		//for(int i = 0; i < probs.size(); i++){
		//	if (d < probs.get(i)){
		//		return nodes.get(i);
		//	}
		//	d -= probs.get(i);
		//}
		// throw new RuntimeException("Bug! shouldn't be here");
		return nodes.get(rnd.nextInt(nodes.size()));
	}
	
	protected void updateProbs(){
		//probs.clear();
		//Double s = nodes.stream().mapToDouble((n)->n.getSubTreeSize()).sum();
		//nodes.forEach((n)->probs.add((double)n.getSubTreeSize()));
		//for(int i = 0; i < probs.size(); i++){
		//	probs.set(i, probs.get(i) / s);
		//}
	}
	
	public void forEachCluster(Consumer<Node<T>> consumer){
		nodes.forEach(consumer);
	}
}
