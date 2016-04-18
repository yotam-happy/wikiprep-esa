package edu.wiki.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

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
	
	ESASearcher searcher;
	ClusteringAlgorithm<ArrayListConceptVector> kMeans = null;
	Map<Integer,Tuple<Integer,Double>> allMappings = null;
	List<ArrayListConceptVector> vectors = null;
	
	public ClustersBuilder(){
		searcher = new ESASearcher();
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
		kMeans = AlgorithmConstructor.getKMeans(20, 0.00001);
		
		kMeans.doClustering(nClusters, nClusters, vectors);
		List<Cluster<ArrayListConceptVector>> clusters = kMeans.getClusters();

		allMappings = new HashMap<>();
		Counting counter = new Counting(10000, "Final mapping.");

		forEachConcept((id,v)->{
			IConceptVector vec;
			try {
				vec = searcher.getConceptESAVector(v);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			vec = ESASearcher.getNormalVector(vec, 350);
			ArrayListConceptVector fastV = new ArrayListConceptVector(vec);
			allMappings.put(id, classifyToClusters(fastV, clusters));
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
		forEachConcept((id,v)->{
			IConceptVector vec;
			try {
				vec = searcher.getConceptESAVector(v);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			vec = ESASearcher.getNormalVector(vec, 250);
			ArrayListConceptVector fastV = new ArrayListConceptVector(vec);
			fastV.normalizeLength();
			fastV.setId(id);
			
        	if (InlinkQueryOptimizer.getInstance().doQuery(id) < minInlinksToParticipate){
        		return;
        	}
			vectors.add(fastV);
			
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
	
	public static void forEachConcept(BiConsumer<Integer,byte[]> consumer){
		try{
			Statement stmt = WikiprepESAdb.getInstance().getConnection()
					.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(Integer.MIN_VALUE);
			ResultSet rs = null;
			try {
				stmt.execute(ConceptESAVectorQueryOptimizer.getInstance().getLoadAllQuery());
		        rs = stmt.getResultSet();
		        while(rs.next()) {
		        	consumer.accept(rs.getInt(1), rs.getBytes(2));
		        }
		        rs.close();
	        	stmt.close();
			}catch(SQLException e) {
				if(rs != null){
					rs.close();
				}
		        if (stmt != null) {
		        	stmt.close();
		        }
				throw new RuntimeException(e);
			}
		}catch(SQLException e){
			throw new RuntimeException(e);
		}
	}
}
