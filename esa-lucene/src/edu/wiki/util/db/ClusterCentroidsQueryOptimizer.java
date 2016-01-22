package edu.wiki.util.db;


public class ClusterCentroidsQueryOptimizer extends AbstractClusterCentroidQueryOptimizer {
	private static ClusterCentroidsQueryOptimizer instance;
	
	public static ClusterCentroidsQueryOptimizer getInstance() {
		if (instance == null) {
			instance = new ClusterCentroidsQueryOptimizer();
		}
		return instance;
	}

	private ClusterCentroidsQueryOptimizer() {
		super("cluster_membership");
	}
}
