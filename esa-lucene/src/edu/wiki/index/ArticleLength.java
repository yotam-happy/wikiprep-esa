package edu.wiki.index;

import java.io.IOException;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import edu.wiki.util.Tuple;
import edu.wiki.util.WikiprepESAdb;
import edu.wiki.util.db.ClusterMembershipQueryOptimizer;

public class ArticleLength {
	public static void main(String[] args) throws SQLException, IOException{
		Statement stmt = WikiprepESAdb.getInstance().getConnection().createStatement();
		stmt = WikiprepESAdb.getInstance().getConnection()
				.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		stmt.execute("SELECT old_id, old_text FROM text");
		ResultSet rs = stmt.getResultSet();
		WikipediaAnalyzer analyzer = new WikipediaAnalyzer();
		Map<Integer,Tuple<Double,Double>> articleLengths = new HashMap<Integer, Tuple<Double,Double>>();
		System.out.println("Scanning articles");
		int c = 0;
		while(rs.next()){
			int id = rs.getInt(1); 
			String text = new String(rs.getBytes(2), "UTF-8");
			double len = 0;
			TokenStream ts = analyzer.tokenStream("contents",new StringReader(text));
	        ts.reset();
	        Map<String, Integer> freqs = new HashMap<>();
	        while (ts.incrementToken()) { 
	            String t = ts.getAttribute(TermAttribute.class).term();
	            Integer f = freqs.get(t);
	            f = (f != null ? f : 0) + 1;
	            freqs.put(t,f);
	            len += 1;
	        }
	        double topFreq = freqs.values().stream().max(Integer::compare).orElse(0);
	        articleLengths.put(id, new Tuple<Double,Double>(len,topFreq/len));
	        if (c % 1000 == 0){
	    		System.out.println("Done " + c);
	        }
	        c++;
	        
		}
		analyzer.close();
		rs.close();
		stmt.close();

		ClusterMembershipQueryOptimizer query = ClusterMembershipQueryOptimizer.getInstance(args[0]);
		query.loadAll();
		Map<Integer,Tuple<Double,Integer>> clusterLengths = new HashMap<>();
		query.forEach((id,m)->{
			Tuple<Double,Integer> t = clusterLengths.get(m.cluster);
			t = new Tuple<>((t==null?0.0:t.x) + (articleLengths.get(id)==null?0.0:articleLengths.get(id).x),
					(t==null?0:t.y) + 1);
			clusterLengths.put(m.cluster, t);
		});
		
		System.out.println("Preparing tables");
		stmt = WikiprepESAdb.getInstance().getConnection().createStatement();
		stmt.execute("DROP TABLE IF EXISTS article_lengths");
		stmt.execute("CREATE TABLE article_lengths (" +
				"id INT(10), " +
				"top_freq INT(10), " +
				"len FLOAT)");
		stmt.execute("DROP TABLE IF EXISTS cluster_lengths");
		stmt.execute("CREATE TABLE cluster_lengths (" +
				"id INT(10), " +
				"size INT(10), " +
				"len FLOAT)");
		
		System.out.println("Saving to db");
		WikiprepESAdb.getInstance().getConnection().setAutoCommit(false);
		String strInsert = "INSERT INTO article_lengths (id,len,top_freq) VALUES (?,?,?)";
		PreparedStatement pstmtWrite = WikiprepESAdb.getInstance().getConnection().prepareStatement(strInsert);
		articleLengths.forEach((id,data)->{
	    	try {
				pstmtWrite.setInt(1, id);
		    	pstmtWrite.setDouble(2, data.x);
		    	pstmtWrite.setDouble(3, data.y);
		    	pstmtWrite.execute();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
		String strInsert2 = "INSERT INTO cluster_lengths (id,size,len) VALUES (?,?,?)";
		PreparedStatement pstmtWrite2 = WikiprepESAdb.getInstance().getConnection().prepareStatement(strInsert2);
		clusterLengths.forEach((id,t)->{
	    	try {
				pstmtWrite2.setInt(1, id);
		    	pstmtWrite2.setInt(2, t.y);
		    	pstmtWrite2.setDouble(3, t.x);
		    	pstmtWrite2.execute();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});

		WikiprepESAdb.getInstance().getConnection().setAutoCommit(true);
		WikiprepESAdb.getInstance().getConnection().close();
	}
}
