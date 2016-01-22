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
import java.util.function.BiFunction;
import java.util.function.Function;





import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.clustering.kmeans.Cluster;
import edu.wiki.clustering.kmeans.KMeans;
import edu.wiki.concept.ArrayListConceptVector;
import edu.wiki.search.ESASearcher;
import edu.wiki.util.Tuple;
import edu.wiki.util.WikiprepESAdb;
import edu.wiki.util.counting.Counting;
import edu.wiki.util.db.ConceptESAVectorQueryOptimizer;
import edu.wiki.util.db.InlinkQueryOptimizer;


public class ClustersBuilder {
	static int MIN_INLINKS_TO_PARTICIPATE = 40;
	static int MAX_TERMS_PER_VECTOR = 1000;

	static int count;
	public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
		ESASearcher searcher = new ESASearcher();
		List<ArrayListConceptVector> vectors = new ArrayList<>();
		InlinkQueryOptimizer.getInstance().loadAll();
		
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
			vec = searcher.getNormalVector(vec, 250);
			ArrayListConceptVector fastV = new ArrayListConceptVector(vec);
			fastV.normalizeLength();
			fastV.setId(id);
			
        	if (InlinkQueryOptimizer.getInstance().doQuery(id) < MIN_INLINKS_TO_PARTICIPATE){
        		return;
        	}
			vectors.add(fastV);
			
			count++;
			if (count % 10000 == 0) {
				System.out.println("loaded " + count);
			}
		});
		
		BiFunction<ArrayListConceptVector, ArrayListConceptVector, Double> metric = (c1,c2) -> {
			return searcher.getCosineDistanceFast(c1, c2);
		};
		
		Function<List<ArrayListConceptVector>, ArrayListConceptVector> centroidCalc = (arr) -> {
			
			ArrayListConceptVector centroid = ArrayListConceptVector.
					merge(arr.toArray(new ArrayListConceptVector[arr.size()]));
			if (centroid == null){
				return null;
			}
			centroid.multipty((float)(1.0 / arr.size()));
			centroid = new ArrayListConceptVector(
					searcher.getNormalVector(centroid, 50));
			return centroid;
		};
		
		System.out.println("Donig KMeans clustering");
		KMeans<ArrayListConceptVector> kMeans = new KMeans<>(4000, vectors, metric, centroidCalc, 20);
		kMeans.calculate();

		Map<Integer,Tuple<Integer,Double>> allMappings = new HashMap<>();
		Counting counter = new Counting(10000, "Loading concepts.");
		forEachConcept((id,v)->{
			IConceptVector vec;
			try {
				vec = searcher.getConceptESAVector(v);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			vec = searcher.getNormalVector(vec, 150);
			ArrayListConceptVector fastV = new ArrayListConceptVector(vec);
			allMappings.put(id, kMeans.getBestCluster(fastV));
			counter.addOne();
		});
		
		// save to db
		String clustersCentroidsTable = args[0] + "_centroids";
		String documentMembershipTable = args[0] + "_membership";

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
				(args[1].equals("terms") ? "concept VARBINARY(256), ":"concept INT(10), ") +
				"cluster INT(10), " +
				"distance FLOAT)" +
				(args[1].equals("terms") ? "" : " DEFAULT CHARSET=binary"));
		stmt.close();
		
		System.out.println("Saving clusters to db...");
		String strVectorInsert = "INSERT INTO "+clustersCentroidsTable+" (id,vector) VALUES (?,?)";
		PreparedStatement pstmtWrite = WikiprepESAdb.getInstance().getConnection().prepareStatement(strVectorInsert);
		for(Cluster<ArrayListConceptVector> cluster : kMeans.getClusters()) {
	    	ByteArrayOutputStream baos = new ByteArrayOutputStream(100000);
	    	DataOutputStream tmpdos = new DataOutputStream(baos);
	    	
	    	ArrayListConceptVector centroid = new ArrayListConceptVector(
	    			searcher.getNormalVector(cluster.getCentroid(), 1000));
	    	
	    	tmpdos.writeInt(centroid.count());
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
		        rs.close();
		        stmt.close();
				throw new RuntimeException(e);
			}
		}catch(SQLException e){
			throw new RuntimeException(e);
		}
	}
}
