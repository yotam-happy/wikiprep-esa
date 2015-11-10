package edu.wiki.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.search.ESAMultiResolutionSearcher;
import edu.wiki.util.WikiprepESAdb;
import edu.wiki.util.db.Concept2ndOrderQueryOptimizer;
import edu.wiki.util.db.TermQueryOptimizer;

/**
 * This performs ESA on the wikipedia article texts themselves.
 * @author yotamesh
 *
 */
public class ESAAnalyzeSelf {
	static int MAX_TERMS_PER_VECTOR = 1000;
	static int BATCH_SIZE = 10000;
	static String strArticleQuery = "SELECT old_id, old_text FROM text LIMIT ?,?";
	static String strVectorInsert = "INSERT INTO concept_esa_vectors (id,vector) VALUES (?,?)";
	  
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		// Reset 2nd order vectors table
		System.out.println("Preparing tables...");
		Statement stmt = WikiprepESAdb.getInstance().getConnection().createStatement();
		stmt.execute("DROP TABLE IF EXISTS concept_esa_vectors");
		stmt.execute("CREATE TABLE concept_esa_vectors (" +
				"id INT(10)," +
				"vector MEDIUMBLOB " +
				") DEFAULT CHARSET=binary");
		
		System.out.println("count article texts...");
		Statement stmtLimit = WikiprepESAdb.getInstance().getConnection().createStatement();
		ResultSet res = stmtLimit.executeQuery("SELECT count(old_id) FROM text");
		res.next();
		int countArticles = res.getInt(1);
		
		ESAMultiResolutionSearcher searcher = new ESAMultiResolutionSearcher();
		System.out.println("in-memory cache tables...");
		Concept2ndOrderQueryOptimizer.getInstance().loadAll();
		TermQueryOptimizer.getInstance().loadAll();

		System.out.println("start working...");
		PreparedStatement pstmt = WikiprepESAdb.getInstance().getConnection().prepareStatement(strArticleQuery);
		PreparedStatement pstmtWrite = WikiprepESAdb.getInstance().getConnection().prepareStatement(strVectorInsert);

		int c = 0;
		while (c < countArticles) {
			pstmt.setInt(1, c);
			pstmt.setInt(2, BATCH_SIZE);
			pstmt.execute();
			ResultSet rs = pstmt.getResultSet();
			while(rs.next()) {
				int conceptId = rs.getInt(1);
				String articleText = new String(rs.getBytes(2), "UTF-8");
				IConceptVector vector = searcher.getConceptVectorUsingMultiResolution(articleText, 1000, true);

				// create a byte stream from the vector
		    	ByteArrayOutputStream baos = new ByteArrayOutputStream(100000);
		    	DataOutputStream dos = new DataOutputStream(baos);
		    	// prune this vector at 1000
		    	int max = vector.size() < MAX_TERMS_PER_VECTOR ? vector.size() : MAX_TERMS_PER_VECTOR; 
		    	dos.writeInt(max);
		    	int count = 0;
		    	IConceptIterator iter = vector.orderedIterator();
		    	while(iter.next()) {
		    		dos.writeInt(iter.getId());
		    		dos.writeFloat((float)iter.getValue());
		    		count++;
		    		if (count >= MAX_TERMS_PER_VECTOR) {
		    			break;
		    		}
		    	}
		    	dos.flush();
				
		    	pstmtWrite.setInt(1, conceptId);
		    	pstmtWrite.setBlob(2, new ByteArrayInputStream(baos.toByteArray()));
		    	pstmtWrite.execute();

		    	c++;
				if (c % 100 == 0) {
					System.out.println("articles done: " + c);
				}
			}
		}
	}
}
