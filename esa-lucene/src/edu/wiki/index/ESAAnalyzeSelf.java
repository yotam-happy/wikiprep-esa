package edu.wiki.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.search.ESAMultiResolutionSearcher;
import edu.wiki.util.WikiprepESAdb;
import edu.wiki.util.db.Concept2ndOrderQueryOptimizer;
import edu.wiki.util.db.IdfQueryOptimizer;
import edu.wiki.util.db.TermQueryOptimizer;

/**
 * This performs ESA on the wikipedia article texts themselves.
 * @author yotamesh
 *
 */
public class ESAAnalyzeSelf {
	static int MAX_TERMS_PER_VECTOR = 1000;
	static String strVectorInsert = "INSERT INTO concept_esa_vectors (id,vector) VALUES (?,?)";
	  
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		File f = new File(args[0]);
		FileOutputStream fos = new FileOutputStream(f);
		DataOutputStream dos = new DataOutputStream(fos);
		// Reset 2nd order vectors table
		System.out.println("Preparing tables...");
		Statement stmt = WikiprepESAdb.getInstance().getConnection().createStatement();
		stmt.execute("DROP TABLE IF EXISTS concept_esa_vectors");
		stmt.execute("CREATE TABLE concept_esa_vectors (" +
				"id INT(10)," +
				"vector MEDIUMBLOB " +
				") DEFAULT CHARSET=binary");
		stmt.close();
		
		ESAMultiResolutionSearcher searcher = new ESAMultiResolutionSearcher();
		System.out.println("in-memory cache tables...");
		Concept2ndOrderQueryOptimizer.getInstance().loadAll();
		TermQueryOptimizer.getInstance().loadAll();
		IdfQueryOptimizer.getInstance().loadAll();

		System.out.println("start working, saving result to tmp file...");

		stmt = WikiprepESAdb.getInstance().getConnection()
				.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		
		int c = 0;
		long byteswritten = 0;
		stmt.execute("SELECT old_id, old_text FROM text");
		ResultSet rs = stmt.getResultSet();
		Instant start = Instant.now();
		while(rs.next()) {
			int conceptId = rs.getInt(1);
			String articleText = new String(rs.getBytes(2), "UTF-8");

			// Dont use short contexts because my computer is too slow....
			IConceptVector vector = searcher.getConceptVectorUsingMultiResolution(articleText, 1000, false, false);
			
			// write concept id
			dos.writeInt(conceptId);
			byteswritten += 4;
			// prune this vector at 1000
	    	int max = vector.size() < MAX_TERMS_PER_VECTOR ? vector.size() : MAX_TERMS_PER_VECTOR; 
	    	// write vector
	    	dos.writeInt(max);
	    	byteswritten += 4;
	    	int count = 0;
	    	// Use orderedIterator sparingly... Only if necessary 
	    	IConceptIterator iter = vector.count() > MAX_TERMS_PER_VECTOR ? vector.orderedIterator() :
	    		vector.iterator();
	    	while(iter.next() && count < MAX_TERMS_PER_VECTOR) {
	    		dos.writeInt(iter.getId());
	    		byteswritten += 4;
	    		dos.writeFloat((float)iter.getValue());
	    		byteswritten += 8;
	    		count++;
	    	}
			
	    	Duration dur = Duration.between(start, Instant.now());
	    	double rate = (double)c / ((double)dur.get(ChronoUnit.SECONDS) / 60.0);
	    	c++;
			System.out.println("articles transformed: " + c + ", avg: " + rate + " articles per minute (" 
					+ byteswritten + " bytes written)");
		}
		dos.close();
		fos.close();
		
		System.out.println("move data from tmp file to db...");
		PreparedStatement pstmtWrite = WikiprepESAdb.getInstance().getConnection().prepareStatement(strVectorInsert);
		// Read data from file to DB (note this cannot be done using LOAD DATA IN FILE
		// which is preferable in general but here varbinary fields can't be loaded that way) 
		FileInputStream fis = new FileInputStream(f);
		DataInputStream dis = new DataInputStream(fis);
		int c2 = 0;
		try {
			while (true) { // loop ends when EOF
				int conceptId = dis.readInt();
				
		    	ByteArrayOutputStream baos = new ByteArrayOutputStream(100000);
		    	DataOutputStream tmpdos = new DataOutputStream(baos);

		    	// Read len and write to stream
		    	int len = dis.readInt();
		    	if (len > MAX_TERMS_PER_VECTOR) {
		    		System.out.println("Something is wrong, got impossible vector len " + len);
		    	}
		    	tmpdos.writeInt(len);
		    	for(int i = 0; i < len; i++) {
		    		tmpdos.writeInt(dis.readInt());
		    		tmpdos.writeFloat(dis.readFloat());
		    	}
		    	tmpdos.flush();

		    	pstmtWrite.setInt(1, conceptId);
		    	pstmtWrite.setBlob(2, new ByteArrayInputStream(baos.toByteArray()));
		    	pstmtWrite.execute();
				
				c2++;
				System.out.println("articles loaded to DB: " + c2);
			}
		}catch (EOFException e) {
			// Done!
		}
		
		if (c != c2) {
			System.out.println("Strange thing happened, written to file less then read... (written " + c + " read " + c2 + ")");
		}
		
		System.out.println("Adding primary key to table");
		stmt = WikiprepESAdb.getInstance().getConnection().createStatement();
		stmt.execute("ALTER TABLE concept_esa_vectors " +
				"CHANGE COLUMN id id INT(10) NOT NULL," +
				"ADD PRIMARY KEY (id)");
		stmt.close();
		WikiprepESAdb.getInstance().getConnection().commit();
		WikiprepESAdb.getInstance().getConnection().close();
	}
}
