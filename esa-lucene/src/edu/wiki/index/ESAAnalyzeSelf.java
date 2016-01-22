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
import java.util.ArrayList;
import java.util.List;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.search.ESAMultiResolutionSearcher;
import edu.wiki.util.Tuple;
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

	static class Task {
		public final int taskId;
		public final List<Tuple<Integer,String>> tuples;
		public Task(int taskId, List<Tuple<Integer,String>> tuples) {
			this.taskId = taskId;
			this.tuples = tuples;
		}
	}
	
	static int THREADS = 4;
	static int BATCH_SIZE = 10;
	static int MAX_TERMS_PER_VECTOR = 1000;
	static String strVectorInsert = "INSERT INTO concept_esa_vectors (id,vector) VALUES (?,?)";
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		String baseFileName = args[0];
		Statement stmt = WikiprepESAdb.getInstance().getConnection().createStatement();
		Init(baseFileName);
		System.out.println("start working, saving result to tmp file...");

		stmt = WikiprepESAdb.getInstance().getConnection()
				.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		
		int c = 0;
		stmt.execute("SELECT old_id, old_text FROM text");
		ResultSet rs = stmt.getResultSet();
		Instant start = Instant.now();
		boolean finished = false;
		while(!finished) {

			// Collect batchs for each thread
			List<Task> tasks = new ArrayList<Task>();
			int k = 0;
			while (k < THREADS && !finished) {
				List<Tuple<Integer,String>> tuples = new ArrayList<Tuple<Integer,String>>();
				tasks.add(new Task(k, tuples));
				int i = 0;
				while (i < BATCH_SIZE) {
					if (!rs.next()) {
						finished = true;
						break;
					}
					i++;
			    	c++;
					Tuple<Integer, String> tuple = new Tuple<Integer, String>(rs.getInt(1), new String(rs.getBytes(2), "UTF-8"));
					tuples.add(tuple);
				}
				k++;
			}
			
			// run k threads
			tasks.parallelStream().forEach((task) -> {
				final FileOutputStream fos;
				try {
					ESAMultiResolutionSearcher searcher = new ESAMultiResolutionSearcher();
					fos = new FileOutputStream(baseFileName + task.taskId, true);
					final DataOutputStream dos = new DataOutputStream(fos);

					task.tuples.stream().forEach((tuple) -> {
						try {
							// Dont use second order....
							IConceptVector vector = searcher.getConceptVectorUsingMultiResolutionForWikipedia(tuple.y, 1000, true, false);
							
							// write concept id
							dos.writeInt(tuple.x);
							// prune this vector at 1000
					    	int max = vector.count() < MAX_TERMS_PER_VECTOR ? vector.count() : MAX_TERMS_PER_VECTOR; 
					    	// write vector
					    	dos.writeInt(max);
					    	int count = 0;
					    	// Use orderedIterator sparingly... Only if necessary 
					    	IConceptIterator iter = vector.count() > MAX_TERMS_PER_VECTOR ? 
					    			vector.bestKOrderedIterator(MAX_TERMS_PER_VECTOR) : vector.iterator();
					    	while(iter.next() && count < MAX_TERMS_PER_VECTOR) {
					    		dos.writeInt(iter.getId());
					    		dos.writeFloat((float)iter.getValue());
					    		count++;
					    	}
						} catch(Exception e) {
							try {
								dos.close();
							} catch (IOException e2) {
								throw new RuntimeException(e2);
							}
							throw new RuntimeException(e);
						}
					});
					dos.close();
					fos.close();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
			
			
	    	Duration dur = Duration.between(start, Instant.now());
	    	double rate = (double)c / ((double)dur.get(ChronoUnit.SECONDS) / 60.0);
			System.out.println("articles transformed: " + c + ", avg: " + rate);
		}
		
		System.out.println("total articles transformed: " + c); 
		Finish(baseFileName);
		
	}
	
	private static void Init(String baseFilename) throws SQLException, ClassNotFoundException, IOException {
		System.out.println("in-memory cache tables...");
		Concept2ndOrderQueryOptimizer.getInstance().loadAll();
		TermQueryOptimizer.getInstance().loadAll();
		IdfQueryOptimizer.getInstance().loadAll();
		
		for (int i = 0; i < THREADS; i++) {
			File f = new File(baseFilename + i);
			if (f.exists()) {
				f.delete();
			}
		}
	}
	
	private static void Finish(String baseFilename) throws IOException, SQLException {
		// Reset 2nd order vectors table
		System.out.println("Preparing tables...");
		Statement stmt = WikiprepESAdb.getInstance().getConnection().createStatement();
		stmt.execute("DROP TABLE IF EXISTS concept_esa_vectors");
		stmt.execute("CREATE TABLE concept_esa_vectors (" +
				"id INT(10)," +
				"vector MEDIUMBLOB " +
				") DEFAULT CHARSET=binary");
		stmt.close();

		WikiprepESAdb.getInstance().getConnection().setAutoCommit(false);
		System.out.println("move data from tmp file to db...");
		PreparedStatement pstmtWrite = WikiprepESAdb.getInstance().getConnection().prepareStatement(strVectorInsert);
		// Read data from file to DB (note this cannot be done using LOAD DATA IN FILE
		// which is preferable in general but here varbinary fields can't be loaded that way) 
		int c = 0;
		for (int i = 0; i < THREADS; i++) {
			FileInputStream fis = new FileInputStream(baseFilename + i);
			DataInputStream dis = new DataInputStream(fis);
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
			    	for(int j = 0; j < len; j++) {
			    		tmpdos.writeInt(dis.readInt());
			    		tmpdos.writeFloat(dis.readFloat());
			    	}
			    	tmpdos.flush();
	
			    	pstmtWrite.setInt(1, conceptId);
			    	pstmtWrite.setBlob(2, new ByteArrayInputStream(baos.toByteArray()));
			    	pstmtWrite.execute();
					
					c++;
					if (c % 10000 == 0) {
						System.out.println("articles loaded to DB: " + c);
					}
				}
			}catch (EOFException e) {
				// Done!
			}
			dis.close();
			fis.close();
		}
		System.out.println("total articles loaded to db: " + c); 

		WikiprepESAdb.getInstance().getConnection().commit();
		WikiprepESAdb.getInstance().getConnection().setAutoCommit(true);
		System.out.println("Adding primary key to table");
		stmt = WikiprepESAdb.getInstance().getConnection().createStatement();
		stmt.execute("ALTER TABLE concept_esa_vectors " +
				"CHANGE COLUMN id id INT(10) NOT NULL," +
				"ADD PRIMARY KEY (id)");
		stmt.close();
		WikiprepESAdb.getInstance().getConnection().close();
	}
}
