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
import java.util.Map.Entry;

import edu.wiki.util.WikiprepESAdb;

/**
 * Calculates 2nd order vectors.
 * This should be called after Initial ESA back end is set up
 * (ESAWikipediaIndexer,IndexModifier all executed)
 * 
 * Note this caches inlinks, pagelinks, idx tables in memory.
 * for enwiki 2015 5Gb heap was enough
 * 
 * @author Yotam Eshel <yotam.happy@gmail.com>
 *
 */
public class ESA2ndOrderVectorCalculator {

	static int MAX_TERMS_PER_VECTOR = 1000;
	static String strVectorQuery = "INSERT INTO concept_2nd (id,vector) VALUES (?,?)";

	public static void main(String[] args) throws SQLException, IOException {

		WikiprepESAdb.getInstance().getConnection().setAutoCommit(false);
		PreparedStatement pstmt = WikiprepESAdb.getInstance().getConnection().prepareStatement(strVectorQuery);

		System.out.println("Loading inlinks table");
		Statement stmt = WikiprepESAdb.getInstance().getConnection().createStatement(
				java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
		Map<Integer,Integer> allInlinkCounts = new HashMap<Integer,Integer>();
		stmt.setFetchSize(Integer.MIN_VALUE);
		stmt.execute("SELECT target_id, inlink FROM inlinks");
		ResultSet rs = stmt.getResultSet();
		int count = 0;
		while (rs.next()) {
			allInlinkCounts.put(rs.getInt(1), rs.getInt(2));
			if (count % 100000 == 0) {
				System.out.println("Loaded " + count);
			}
			count++;
		}		
		rs.close();
		stmt.close();
		System.out.println("Loading pagelinks table");
		stmt = WikiprepESAdb.getInstance().getConnection().createStatement();
		Map<Integer,List<Integer>> allLinks = new HashMap<Integer,List<Integer>>(
				java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		stmt.execute("SELECT source_id, target_id FROM pagelinks");
		rs = stmt.getResultSet();
		count = 0;
		while (rs.next()) {
			List<Integer> targets = allLinks.get(rs.getInt(1));
			if (targets == null) {
				targets = new ArrayList<Integer>();
				allLinks.put(rs.getInt(1), targets);
			}
			targets.add(rs.getInt(2));
			if (count % 1000000 == 0) {
				System.out.println("Loaded " + count);
			}
			count++;
		}		
		rs.close();
		stmt.close();
		
		// Reset 2nd order vectors table
		System.out.println("Preparing tables...");
		stmt = WikiprepESAdb.getInstance().getConnection().createStatement();
		stmt.execute("DROP TABLE IF EXISTS concept_2nd");
		stmt.execute("CREATE TABLE concept_2nd (" +
				"id INT(10)," +
				"vector MEDIUMBLOB " +
				") DEFAULT CHARSET=binary");
		
		// Go over all terms
		stmt.execute("SELECT id FROM article");
		rs = stmt.getResultSet();
		int c = 0;
		while (rs.next()) {
			int conceptId = rs.getInt(1);

			Map<Integer, Float> vector2ndOrder = new HashMap<Integer, Float>();
			
			if (allLinks.get(conceptId) == null) {
				continue;
			}
			// iterate over linked concepts from current concept
			for (Integer linkedConcept : allLinks.get(conceptId)) {
				// test "concept generality"
				if (Math.log(allInlinkCounts.get(linkedConcept)) - Math.log(allInlinkCounts.get(conceptId)) >= 1.0) {
					// if the concept is indeed more general then give it the bonus
					vector2ndOrder.put(linkedConcept, 1.0f);
				}
			}

			// maybe some terms don't generate any bonus ?
			if (vector2ndOrder.size() > 0) {
				
				// create a byte stream from the vector
		    	ByteArrayOutputStream baos = new ByteArrayOutputStream(100000);
		    	DataOutputStream dos = new DataOutputStream(baos);
		    	// prune this vector at 1000
		    	int max = vector2ndOrder.size() < MAX_TERMS_PER_VECTOR ? vector2ndOrder.size() : MAX_TERMS_PER_VECTOR; 
		    	dos.writeInt(max);
		    	count = 0;
		    	for(Entry<Integer, Float> e : vector2ndOrder.entrySet()) {
		    		dos.writeInt(e.getKey());
		    		dos.writeFloat(e.getValue());
		    		count++;
		    		if (count >= MAX_TERMS_PER_VECTOR) {
		    			break;
		    		}
		    	}
		    	dos.flush();

		    	pstmt.setInt(1, conceptId);
		    	pstmt.setBlob(2, new ByteArrayInputStream(baos.toByteArray()));
		    	pstmt.execute();

		    	
			}
			
			c ++;
			if (c % 1000 == 0) {
				WikiprepESAdb.getInstance().getConnection().commit();
				System.out.println("done " + c + " terms");
			}
		}
		WikiprepESAdb.getInstance().getConnection().commit();
		pstmt.close();

		System.out.println("Adding primary key to table");
		stmt = WikiprepESAdb.getInstance().getConnection().createStatement();
		stmt.execute("ALTER TABLE concept_2nd " +
				"CHANGE COLUMN id id INT(10) NOT NULL," +
				"ADD PRIMARY KEY (id)");
		stmt.close();
		WikiprepESAdb.getInstance().getConnection().commit();
		WikiprepESAdb.getInstance().getConnection().close();
	}
}
