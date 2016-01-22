package edu.wiki.clustering.kmeans;

/* 
 * KMeans.java ; Cluster.java ; Point.java
 *
 * Solution implemented by DataOnFocus
 * www.dataonfocus.com
 * 2015
 *
*/
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Function;

import edu.wiki.util.Tuple;
import edu.wiki.util.counting.Counting;

public class KMeans<T> {

    private List<T> points;
    private List<Cluster<T>> clusters;
    
    private BiFunction<T, T, Double> metric;
    private Function<List<T>, T> centroidCalc;
    
    private int maxIterations;
    public KMeans(int numClusters, List<T> points, BiFunction<T, T, Double> metric, Function<List<T>, T> centroidCalc, int maxIterations) {
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
        	for(Iterator<Cluster<T>> it = clusters.iterator(); it.hasNext(); ){
        		Cluster<T> cluster = it.next();
                T centroid = centroidCalc.apply(cluster.getPoints());
                if(centroid != null) {
                	cluster.setCentroid(centroid);
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
    
    public Tuple<Integer,Double> getBestCluster(T point){
    	return clusters.stream()
	    	.map((cluster)->new Tuple<Integer,Double>(cluster.id,metric.apply(point, cluster.getCentroid())))
	    	.min((e1,e2)->Double.compare(e1.y, e2.y)).orElse(null);
    }
    
    private void assignCluster(boolean partial) {
    	Counting counter = new Counting(10000, "Assigning clusters"); 
        points.parallelStream()
        .forEach((point)->{
    		Tuple<Integer,Double> t = getBestCluster(point);
            if (t != null){
            	clusters.get(t.x).addPoint(point);
            }
            counter.addOne();
        });
    }
}