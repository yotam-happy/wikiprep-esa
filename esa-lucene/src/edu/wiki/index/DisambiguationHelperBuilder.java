package edu.wiki.index;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import edu.wiki.search.DisambiguatingText2Features;
import edu.wiki.util.Tuple;
import edu.wiki.util.WikiprepESAUtils;
import edu.wiki.util.WikiprepESAdb;
import edu.wiki.util.db.ArticleQueryOptimizer;
import edu.wiki.util.db.ConceptESAVectorQueryOptimizer;
import edu.wiki.util.db.IdfQueryOptimizer;
import edu.wiki.util.db.TermQueryOptimizer;


public class DisambiguationHelperBuilder {
	static class Task {
		public final int taskId;
		public final List<Tuple<Integer,String>> tuples;
		public Task(int taskId, List<Tuple<Integer,String>> tuples) {
			this.taskId = taskId;
			this.tuples = tuples;
		}
	}
	
	static int THREADS = 8;
	static int BATCH_SIZE = 4;
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
		long byteswritten = 0;
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
					fos = new FileOutputStream(baseFileName + task.taskId, true);
					final DataOutputStream dos = new DataOutputStream(fos);

					task.tuples.stream().forEach((tuple) -> {
						try {
							DisambiguatingText2Features disambiguatingText2Features = new DisambiguatingText2Features();
							// get disambiguation contexts
							Collection<String> contexts = WikiprepESAUtils.getWikipediaDocumentParagraph(tuple.y);
							Map<Integer, Double > features = 
									disambiguatingText2Features.getDisambiguatingFeatures(contexts.stream());
							if (features.isEmpty()) {
								dos.close();
								return;
							}
							
							// write concept id
							dos.writeInt(tuple.x);
					    	// write disambiguation features
					    	dos.writeInt(features.size());
					    	features.entrySet().forEach((e) -> {
								try {
						    		dos.writeInt(e.getKey());
						    		dos.writeDouble(e.getValue());
								} catch(Exception e2) {
									throw new RuntimeException(e2);
								}
					    	});
						} catch(Exception e) {
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
			System.out.println("articles analyzed: " + c + ", avg: " + rate + " articles per minute (" 
					+ byteswritten + " bytes written)");
		}
		
		System.out.println("total articles analyzed: " + c); 

		
	}
	
	private static void Init(String baseFilename) throws SQLException, ClassNotFoundException, IOException {
//		System.out.println("loading Concept2ndOrderQueryOptimizer");
//		Concept2ndOrderQueryOptimizer.getInstance().loadAll();
		System.out.println("loading TermQueryOptimizer");
		TermQueryOptimizer.getInstance().loadAll();
		System.out.println("loading IdfQueryOptimizer");
		IdfQueryOptimizer.getInstance().loadAll();
		System.out.println("loading ConceptESAVectorQueryOptimizer");
		ConceptESAVectorQueryOptimizer.getInstance().loadAll();
		System.out.println("loading ArticleQueryOptimizer");
		ArticleQueryOptimizer.getInstance().loadAll();
		
		for (int i = 0; i < THREADS; i++) {
			File f = new File(baseFilename + i);
			if (f.exists()) {
				f.delete();
			}
		}
	}
}
