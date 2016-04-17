package edu.wiki.clustering.kmeans;

/* 
 * KMeans.java ; Cluster.java ; Point.java
 *
 * Solution implemented by DataOnFocus
 * www.dataonfocus.com
 * 2015
 *
*/
import edu.wiki.util.Tuple;
import edu.wiki.util.counting.Counting;
import gnu.trove.TIntHashSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KMeans<T> {

	private Map<Integer,TIntHashSet> mustLink = null;
	private Map<Integer,TIntHashSet> cannotLink = null;
	private Map<Integer,T> idMapping;
    Function<T,Integer> getIdFunc = null;
	
    private List<T> points;
    private List<Cluster<T>> clusters;
    
    private BiFunction<T, T, Double> metric;
    private Function<List<T>, T> centroidCalc;
    
    
    private int maxIterations;
    public KMeans(int numClusters, 
    		List<T> points, 
    		BiFunction<T, T, Double> metric, 
    		Function<List<T>, T> centroidCalc, 
    		int maxIterations) {
    	this.maxIterations = maxIterations;
    	this.points = points;
    	this.clusters = new ArrayList<Cluster<T>>();    	
    	this.metric = metric; 
    	this.centroidCalc = centroidCalc;
    	
    	Random rnd = new Random(System.currentTimeMillis());
    	//Initialize clusters with random centroids
    	for (int i = 0; i < numClusters; i++) {
    		Cluster<T> cluster = new Cluster<T>(i);
    		T centroid = points.get(rnd.nextInt(points.size()));
    		cluster.setCentroid(centroid);
    		clusters.add(cluster);
    	}
    }
    
    
    public void addMustLinkConstrain(int id1, int id2) {
    	if (!mustLink.containsKey(id1)){
    		mustLink.put(id1, new TIntHashSet());
    	}
    	if (mustLink.containsKey(id2)){
    		mustLink.put(id1, new TIntHashSet());
    	}
    	mustLink.get(id1).forEach((i)->mustLink.get(i).add(id2));
    	mustLink.get(id2).forEach((i)->mustLink.get(i).add(id1));
    	mustLink.get(id1).add(id2);
    	mustLink.get(id2).add(id1);
    }
    public void addCannotLinkConstrain(int id1, int id2) {
    	if (!cannotLink.containsKey(id1)){
    		cannotLink.put(id1, new TIntHashSet());
    	}
    	if (!cannotLink.containsKey(id2)){
    		cannotLink.put(id2, new TIntHashSet());
    	}
    	if (mustLink.containsKey(id1)) {
    		mustLink.get(id1).forEach((i)->cannotLink.get(i).add(id2));
    	}
    	if (mustLink.containsKey(id2)) {
    		mustLink.get(id2).forEach((i)->cannotLink.get(i).add(id1));
    	}
    	cannotLink.get(id1).add(id2);
    	cannotLink.get(id2).add(id1);
    }
    
    public void setConstraints(Set<Tuple<Integer,Integer>> mustLink, 
    		Set<Tuple<Integer,Integer>> cannotLink, 
    		Function<T,Integer> getId){
    	idMapping = new HashMap<>();
    	points.forEach((p)->idMapping.put(getId.apply(p), p));
    	
    	this.mustLink = new HashMap<>();
    	this.cannotLink = new HashMap<>();
    	this.getIdFunc = getId;
    	mustLink.forEach((t)->addMustLinkConstrain(t.x, t.y));
    	cannotLink.forEach((t)->addCannotLinkConstrain(t.x, t.y));
    }
    
    public List<Cluster<T>> getClusters() {
    	return clusters;
    }
    
	//The process to calculate the K Means, with iterating method.
    public void calculate() {
        boolean finish = false;
        int iteration = 0;
        
        // Add in new data, one at a time, recalculating centroids with each new one. 
        while(!finish) {
        	//Clear cluster state
        	clearClusters();
        	
        	//Assign points to the closer cluster
        	assignCluster(true);
            
            //Calculate new centroids.
        	int cid = 0;
        	for(Iterator<Cluster<T>> it = clusters.iterator(); it.hasNext(); ){
        		Cluster<T> cluster = it.next();
                T centroid = centroidCalc.apply(cluster.getPoints());
                if(centroid != null) {
                	cluster.setCentroid(centroid);
                	cluster.setId(cid);
                	cid++;
                } else {
                	it.remove();
                }
            }
        	
        	iteration++;
        	System.out.println("#################");
        	System.out.println("Iteration: " + iteration);
        	
        	// not quite sure about this
        	if(iteration > maxIterations) {
        		finish = true;
        	}
        }
        
    	System.out.println("Done iterations... Now assign all docs and compute clusters again");
        // Now assign all points
    	assignCluster(false);
    	
        //recalculate final centroids.
    	clusters.parallelStream().forEach((cluster)->{
        	cluster.setCentroid(centroidCalc.apply(cluster.getPoints()));
    	});
    	// remove redundant ones
    	for(Iterator<Cluster<T>> it = clusters.iterator(); it.hasNext(); ){
    		Cluster<T> cluster = it.next();
            if(cluster.centroid == null) {
            	it.remove();
            }
        }
        
    }
    
    private void clearClusters() {
    	for(Cluster<T> cluster : clusters) {
    		cluster.clear();
    	}
    }
    
    private int countConstraintViolations(int pointId, Cluster<T> cluster) {
    	int violations = 0;
    	// must link
    	if (mustLink.containsKey(pointId)){
    		throw new RuntimeException("must link constraint not supported");
    		// TODO: this is wrong! need to check only points that
    		// are already assigned
//    		TIntIterator it = mustLink.get(pointId).iterator();
//    		while(it.hasNext()){
//    			if (!cluster.getPoints().contains(it.next())){
//    				violations++;
//    			}
//    		}
    	}
    	// cannot link
    	if (cannotLink.containsKey(pointId)){
    		Iterator<T> it = cluster.getPoints().iterator();
    		while(it.hasNext()){
    			T t = it.next();
    			int id = getIdFunc.apply(t);
    			if (cannotLink.get(pointId).contains(id)){
    				violations++;
    			}
    		}
    	}
    	return violations;
    }
    
    public Tuple<Integer,Double> getBestCluster(T point){
    	List<Tuple<Integer,Double>> l = clusters.parallelStream()
			.map((cluster)->new Tuple<Integer,Double>(cluster.id,metric.apply(point, cluster.getCentroid())))
	    	.sorted((e1,e2)->Double.compare(e1.y, e2.y)).collect(Collectors.toList());
    	
    	if (getIdFunc == null || getIdFunc.apply(point) == -1){
    		if (l.get(0).y.isNaN()){
    			return null;
    		}
    		return l.get(0);
    	} else {
    		// this is horrible... it is: Tuple<clusterId,Tuple<nViolations,distanceFromCentroid>>
    		Set<Tuple<Integer,Tuple<Integer,Double>>> order = new HashSet<>();
    		
    		for(Tuple<Integer,Double> t : l){
    			int violations = countConstraintViolations(getIdFunc.apply(point), clusters.get(t.x)); 
    			order.add(new Tuple<>(t.x, new Tuple<>(violations, t.y)));
    		}
    		Tuple<Integer,Tuple<Integer,Double>> best = order.stream().sorted((e1,e2)->{
    			int cmp1 = e1.y.x.compareTo(e2.y.x);
    			return cmp1 != 0 ? cmp1 : e1.y.y.compareTo(e2.y.y);
    		}).findFirst().orElse(null);
    		return new Tuple<>(best.x, best.y.y);
    	}
    }
    
    private void assignCluster(boolean partial) {
    	Counting counter = new Counting(10000, "Assigning clusters"); 
        points.stream()
        .forEach((point)->{
    		Tuple<Integer,Double> t = getBestCluster(point);
            if (t != null){
            	clusters.get(t.x).addPoint(point);
            }
            counter.addOne();
        });
    }
}