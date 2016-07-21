package edu.wiki.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import edu.clustering.jot.algorithms.AlgorithmConstructor;
import edu.clustering.jot.interfaces.ClusteringAlgorithm;
import edu.clustering.jot.kmeans.Cluster;
import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.concept.ArrayListConceptVector;
import edu.wiki.search.ESASearcher;
import edu.wiki.util.Tuple;
import edu.wiki.util.WikiprepESAdb;
import edu.wiki.util.counting.Counting;
import edu.wiki.util.db.ConceptESAVectorQueryOptimizer;
import edu.wiki.util.db.InlinkQueryOptimizer;


public class ClustersBuilder {
	private int minInlinksToParticipate = 40;
	static final int MAX_TERMS_PER_VECTOR = 1000;
	static final int MAX_ITERATIONS = 20;
	
	ESASearcher searcher = new ESASearcher();
	ClusteringAlgorithm<ArrayListConceptVector> kMeans = null;
	Map<Integer,Tuple<Integer,Double>> allMappings = null;
	List<ArrayListConceptVector> vectors = null;
	Consumer<BiConsumer<Integer,ArrayListConceptVector>> forEachVector;
	
	public ClustersBuilder(){
		forEachVector = forEachBOW;
	}

	public static void main(String[] args) {
		ClustersBuilder clustersBuilder = new ClustersBuilder();
		clustersBuilder.loadVectors();
		
		int[] nofclusters = {2,4,8,16,32,64,128,256,512,1024,2048};
		for(int n : nofclusters){
			System.out.println("do clustering with " + n + " clusters");
			clustersBuilder.buildClusters(n);
			clustersBuilder.saveClustersToDb(args[0] + n);
		}
	}
	
	public void loadVectors() {
		InlinkQueryOptimizer.getInstance().loadAll();
		vectors = loadConceptVectors();
	}
	
	static int count;
	public void buildClusters(int nClusters) {
		
		System.out.println("Donig KMeans clustering");
		kMeans = AlgorithmConstructor.getKMeansPlusPlus(20, 0.00001);
		
		kMeans.doClustering(nClusters, nClusters, vectors);
		List<Cluster<ArrayListConceptVector>> clusters = kMeans.getClusters();

		allMappings = new HashMap<>();
		Counting counter = new Counting(10000, "Final mapping.");

		forEachVector.accept((id,v)->{
			allMappings.put(id, classifyToClusters(v, clusters));
			counter.addOne();
		});
	}
	
	public Tuple<Integer,Double> classifyToClusters(ArrayListConceptVector p, 
			List<Cluster<ArrayListConceptVector>> cluters){
		return cluters.stream().map((c)->new Tuple<Integer,Double>(c.id,c.getCentroid().distance(p)))
		.max((t1,t2)->Double.compare(t1.y, t2.y)).orElse(null);
	}

	public void saveClustersToDb(String baseTableName){
		try {
			// save to db
			String clustersCentroidsTable = baseTableName + "_centroids";
			String documentMembershipTable = baseTableName + "_membership";
	
			System.out.println("Preparing tables...");
			WikiprepESAdb.getInstance().getConnection().setAutoCommit(false);
			Statement stmt = WikiprepESAdb.getInstance().getConnection().createStatement();
			stmt.execute("DROP TABLE IF EXISTS "+clustersCentroidsTable);
			stmt.execute("CREATE TABLE "+clustersCentroidsTable+" (" +
					"id INT(10)," +
					"vector MEDIUMBLOB " +
					") DEFAULT CHARSET=binary");
			stmt.execute("DROP TABLE IF EXISTS "+documentMembershipTable);
			stmt.execute("CREATE TABLE "+documentMembershipTable+" (" +
					(baseTableName.equals("terms") ? "concept VARBINARY(256), ":"concept INT(10), ") +
					"cluster INT(10), " +
					"distance FLOAT)" +
					(baseTableName.equals("terms") ? "" : " DEFAULT CHARSET=binary"));
			stmt.close();
			
			System.out.println("Saving clusters to db...");
			String strVectorInsert = "INSERT INTO "+clustersCentroidsTable+" (id,vector) VALUES (?,?)";
			PreparedStatement pstmtWrite = WikiprepESAdb.getInstance().getConnection().prepareStatement(strVectorInsert);
			for(Cluster<ArrayListConceptVector> cluster : kMeans.getClusters()) {
		    	ByteArrayOutputStream baos = new ByteArrayOutputStream(100000);
		    	DataOutputStream tmpdos = new DataOutputStream(baos);
		    	
		    	ArrayListConceptVector centroid = new ArrayListConceptVector(
		    			ESASearcher.getNormalVector(cluster.getCentroid(), 1000));
		    	
		    	tmpdos.writeInt(centroid.size());
		    	IConceptIterator it = centroid.iterator();
		    	while(it.next()){
		    		tmpdos.writeInt(it.getId());
		    		tmpdos.writeFloat((float)it.getValue());
		    	}
		    	tmpdos.flush();
		    	
		    	pstmtWrite.setInt(1, cluster.getId());
		    	pstmtWrite.setBlob(2, new ByteArrayInputStream(baos.toByteArray()));
		    	pstmtWrite.execute();
			}
	
	
			pstmtWrite.close();
			System.out.println("Saving clusters membership to db...");
			String strConceptClusterInsert = "INSERT INTO " + documentMembershipTable + " (concept,cluster,distance) VALUES (?,?,?)";
			PreparedStatement pstmtWrite2 = WikiprepESAdb.getInstance().getConnection().prepareStatement(strConceptClusterInsert);
			allMappings.forEach((id,t)->{
		    	try {
					pstmtWrite2.setInt(1, id);
			    	pstmtWrite2.setInt(2, t.x);
			    	pstmtWrite2.setDouble(3, t.y);
			    	pstmtWrite2.execute();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
			pstmtWrite2.close();
			WikiprepESAdb.getInstance().getConnection().commit();
		} catch(SQLException | IOException e){
			throw new RuntimeException(e);
		}
	}

	public List<ArrayListConceptVector> loadConceptVectors(){
		List<ArrayListConceptVector> vectors = new ArrayList<>();
		System.out.println("Loading concepts");
		// Get all concept vectors
		count = 0;
		forEachVector.accept((id,vec)->{
			
        	if (InlinkQueryOptimizer.getInstance().doQuery(id) < minInlinksToParticipate){
        		return;
        	}
			vectors.add(vec);
			
			count++;
			if (count % 10000 == 0) {
				System.out.println("loaded " + count);
			}
		});
		return vectors;
	}
	
	public void setMinInlinksToParticipate(int minInlinksToParticipate){
		this.minInlinksToParticipate = minInlinksToParticipate;
	}

	Consumer<BiConsumer<Integer,ArrayListConceptVector>> forEachESA = 
		(consumer) ->{
			ConceptESAVectorQueryOptimizer.getInstance().forEach((id,v)->{
				IConceptVector vec;
				try {
					vec = searcher.getConceptESAVector(v);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				vec = ESASearcher.getNormalVector(vec, 250);
				ArrayListConceptVector fastV = new ArrayListConceptVector(vec);
				fastV.setId(id);
				
				consumer.accept(id, fastV);
			});
		};
	Consumer<BiConsumer<Integer,ArrayListConceptVector>> forEachBOW = 
		(consumer) ->{
			Map<String,Integer> termIdMap = new HashMap<>();
			WikiprepESAdb.getInstance().forEachResult("SELECT old_id, old_text FROM text", (rs)->{
				try {
					String text = new String(rs.getBytes(2), "UTF-8");
					int conceptId = rs.getInt(1);
					
					HashMap<String,Integer> bow = searcher.getBOW(text, true);
					double nTerms = bow.values().stream().mapToDouble((x)->x).sum();

					HashMap<Integer,Double> v = new HashMap<>();
					bow.forEach((term,termCount)->{
						Integer termId = termIdMap.get(term);
						if(termId == null){
							termId = termIdMap.size();
							termIdMap.put(term, termId);
						}
						v.put(termId, (double)termCount / nTerms);
					});
					
					ArrayListConceptVector fastV = new ArrayListConceptVector(v.size());
					v.forEach((termId,tf)->fastV.add(termId,tf));
					fastV.setId(conceptId);
					
					consumer.accept(conceptId, fastV);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		};				
}
