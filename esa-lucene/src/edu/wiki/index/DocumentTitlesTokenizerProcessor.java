package edu.wiki.index;

import java.io.ByteArrayInputStream;
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

import edu.wiki.util.WikiprepESAdb;

public class DocumentTitlesTokenizerProcessor {

	static final int MAX_TERMS_PER_VECTOR = 1000;
	static final String strVectorUpdate = "UPDATE article SET title_processed=? WHERE id=?";

	static int c = 0;

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {

		WikiprepESAdb.getInstance().getConnection().setAutoCommit(false);
		PreparedStatement pstmt = WikiprepESAdb.getInstance().getConnection().prepareStatement(strVectorUpdate);

		System.out.println("Loading all article titles");
		Statement stmt = WikiprepESAdb.getInstance().getConnection().createStatement(
				java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
		Map<Integer,String> allArticleTitles = new HashMap<Integer,String>();
		stmt.setFetchSize(Integer.MIN_VALUE);
		stmt.execute("SELECT id, title FROM article");
		ResultSet rs = stmt.getResultSet();
		int count = 0;
		while (rs.next()) {
			allArticleTitles.put(rs.getInt(1), new String(rs.getBytes(2), "UTF-8"));
			if (count % 100000 == 0) {
				System.out.println("Loaded " + count);
			}
			count++;
		}		
		rs.close();
		stmt.close();
		WikipediaAnalyzer analyzer = new WikipediaAnalyzer();

		// Prepare tables
		System.out.println("Preparing tables...");
		stmt = WikiprepESAdb.getInstance().getConnection().createStatement();
		try{
			stmt.execute("ALTER TABLE article DROP COLUMN title_processed");
		}catch(Exception e) {
			// this is ok
		}
		stmt.execute("ALTER TABLE article ADD COLUMN title_processed VARBINARY(255) NULL AFTER title");
		stmt.close();

		// Do the updating
		Map<Integer,String> allProcessedTitles = new HashMap<Integer,String>();
		c = 0;
		allArticleTitles.forEach((id, title) -> {
	        TokenStream ts = analyzer.tokenStream("contents",new StringReader(stripTitle(title)));
	        
	        StringBuffer sb = new StringBuffer();
	        sb.append("$");
	        try {
				ts.reset();
		        while (ts.incrementToken()) { 
		            TermAttribute t = ts.getAttribute(TermAttribute.class);
		            sb.append('_').append(t.term());
		        }
			} catch (Exception e) {
				System.out.println("Exception while processing \"" + title + "\" id: " + id);
				e.printStackTrace();
			}
			if (sb.length() > 0) {
				allProcessedTitles.put(id, sb.toString());
			}
	    	try {
		    	pstmt.setBlob(1, new ByteArrayInputStream(sb.toString().getBytes("UTF-8")));
				pstmt.setInt(2, id);
		    	pstmt.execute();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (c % 100000 == 0) {
				System.out.println("Updated " + c);
			}
			c++;
		});
		analyzer.close();
		pstmt.close();

		System.out.println("Creating Index...");
		stmt = WikiprepESAdb.getInstance().getConnection().createStatement();
		stmt.execute("ALTER TABLE article ADD INDEX title_processed (title_processed ASC)");
		stmt.close();

		WikiprepESAdb.getInstance().getConnection().commit();
		WikiprepESAdb.getInstance().getConnection().close();
		System.out.println("Done...");
	}
	
	public static String stripTitle(String title) {
		if (title.contains("(")) {
			title = title.substring(0, title.indexOf('('));
		}
		return title.trim();
	}
}
