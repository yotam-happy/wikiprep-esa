package edu.wiki.util.db;

public class ClusterMembershipQueryOptimizer extends AbstractClusterMembershipQueryOptimizer {
	private static ClusterMembershipQueryOptimizer instance;
	
	public static ClusterMembershipQueryOptimizer getInstance() {
		if (instance == null) {
			instance = new ClusterMembershipQueryOptimizer();
		}
		return instance;
	}

	private ClusterMembershipQueryOptimizer(){
		super("cluster_membership");
	}
}
