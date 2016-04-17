package edu.wiki.util.db;

import java.util.HashMap;
import java.util.Map;


public class ClusterCentroidsQueryOptimizer extends AbstractClusterCentroidQueryOptimizer {
	private static Map<String,ClusterCentroidsQueryOptimizer> instances = new HashMap<>();
	
	public static ClusterCentroidsQueryOptimizer getInstance(String baseTableNme) {
		ClusterCentroidsQueryOptimizer o = instances.get(baseTableNme);
		if (o == null) {
			o = new ClusterCentroidsQueryOptimizer(baseTableNme);
			instances.put(baseTableNme, o);
		}
		return o;
	}

	private ClusterCentroidsQueryOptimizer(String baseTableNme){
		super(baseTableNme + "_centroids");
	}
}
