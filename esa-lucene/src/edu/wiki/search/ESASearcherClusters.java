package edu.wiki.search;

import java.util.HashMap;
import java.util.Map;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.concept.TroveConceptVector;
import edu.wiki.util.db.AbstractClusterMembershipQueryOptimizer.MembershipData;
import edu.wiki.util.db.ArticleLengthQueryOptimizer;
import edu.wiki.util.db.ClusterCentroidsQueryOptimizer;
import edu.wiki.util.db.ClusterLengthQueryOptimizer;
import edu.wiki.util.db.ClusterMembershipQueryOptimizer;
import edu.wiki.util.db.ClusterSizeQueryOptimizer;
import edu.wiki.util.db.ConceptESAVectorQueryOptimizer;
import edu.wiki.util.db.InlinkQueryOptimizer;

public class ESASearcherClusters {
	ESASearcher searcher;
	Map<Integer, IConceptVector> centroids;
	Map<Integer,Double> idf;
	
	public ESASearcherClusters() {
		ClusterCentroidsQueryOptimizer query = ClusterCentroidsQueryOptimizer.getInstance();
		searcher = new ESASearcher();
		query.loadAll();
		centroids = new HashMap<>();

		query.forEach((id, centroidVec) -> {
			try {
				centroids.put(id, searcher.getConceptESAVector(centroidVec));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
		
		idf = calcIdf();
	}

	public Map<Integer,Double> calcIdf() {
		Map<Integer,Double> idf = new HashMap<>();
		for(IConceptVector centroid : centroids.values()) {
			IConceptIterator it = centroid.iterator();
			while(it.next()) {
				idf.put(it.getId(), idf.get(it.getId()) != null ? idf.get(it.getId()) + it.getValue() : it.getValue());
			}
		}
		return idf;
	}
	
	public IConceptVector ClustersFeatureVectorByCentroid(IConceptVector vec) {
		IConceptVector vec2 = new TroveConceptVector(100);
		centroids.forEach((id, c)->vec2.add(id, searcher.getRelatedness(vec, c)));
		return vec2;
	}

	public IConceptVector ClustersFeatureVectorByMembership(IConceptVector vec, int normalize) {
		IConceptVector vec2 = new TroveConceptVector(100);
		ClusterMembershipQueryOptimizer query = ClusterMembershipQueryOptimizer.getInstance();
		
		IConceptIterator it = vec.iterator();
		while(it.next()){
			MembershipData clusterData = query.doQuery(it.getId());
			if (clusterData != null) {
				Double norm = 1.0;
				switch(normalize){
				case 1:
					norm = ArticleLengthQueryOptimizer.getInstance().doQuery(it.getId()) /
							ClusterLengthQueryOptimizer.getInstance().doQuery(clusterData.cluster);
					break;
				case 2:
					norm = ArticleLengthQueryOptimizer.getInstance().doQuery(it.getId()) *
							((double)ClusterSizeQueryOptimizer.getInstance().doQuery(clusterData.cluster) /
							ClusterLengthQueryOptimizer.getInstance().doQuery(clusterData.cluster));
				}
				vec2.add(clusterData.cluster, it.getValue() * norm);
			}
		}
		return vec2;
	}
	
	public IConceptVector getCentroidWithIdf(IConceptVector centroid) {
		IConceptVector v = new TroveConceptVector(100);
		IConceptIterator it = centroid.iterator();
		while(it.next()) {
			v.add(it.getId(), it.getValue() / (1.0 + Math.log(idf.get(it.getId()))));
		}
		return v;
	}
		
	
	public IConceptVector filterUsingSelfVectors(IConceptVector vec) {
		IConceptVector v2 = new TroveConceptVector(100);
		IConceptIterator it = vec.iterator();
		while(it.next()){
			byte[] b = ConceptESAVectorQueryOptimizer.getInstance().doQuery(it.getId());
			IConceptVector v = searcher.getConceptESAVector(b);
			double r = searcher.getRelatedness(vec, v);
			v2.add(it.getId(),r * it.getValue());
		}
		
		return v2;
	}
	public IConceptVector filterTopConcepts(IConceptVector vec, int treshold) {
		IConceptVector v2 = new TroveConceptVector(100);
		IConceptIterator it = vec.iterator();
		while(it.next()){
			Integer i = InlinkQueryOptimizer.getInstance().doQuery(it.getId());
			if (i != null && i > treshold) {
				v2.add(it.getId(),it.getValue());
			}
		}
		return v2;
	}
}
