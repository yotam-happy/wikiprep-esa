package edu.wiki.util.db;

import java.util.HashMap;
import java.util.Map;

public class ClusterMembershipQueryOptimizer extends AbstractClusterMembershipQueryOptimizer {
	private static Map<String,ClusterMembershipQueryOptimizer> instances = new HashMap<>();
	
	public static ClusterMembershipQueryOptimizer getInstance(String baseTableNme) {
		ClusterMembershipQueryOptimizer o = instances.get(baseTableNme);
		if (o == null) {
			o = new ClusterMembershipQueryOptimizer(baseTableNme);
			instances.put(baseTableNme, o);
		}
		return o;
	}

	private ClusterMembershipQueryOptimizer(String baseTableNme){
		super(baseTableNme + "_membership");
	}
}
