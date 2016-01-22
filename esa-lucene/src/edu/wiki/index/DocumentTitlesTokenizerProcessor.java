package edu.wiki.index;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import edu.wiki.util.BulkDbInserter;
import edu.wiki.util.WikiprepESAdb;

public class DocumentTitlesTokenizerProcessor {

	static final int COUNT_THRESHHOLD = 5;
	static final String tableName = "surface_names";
	static final String[] tableColumns = {"name", "concept_id"};

	static int c = 0;

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {

		Map<String,Map<Integer,Integer>> allSurfaceNames = new HashMap<>();
		BulkDbInserter<Object[]> bulkDbInserter = new BulkDbInserter<Object[]>(tableName, tableColumns) {
			@Override
			protected void setRowData(PreparedStatement pstmt, Object[] data, int columnStartIndex) throws SQLException {
		    	if (data == null) {
					pstmt.setNull(columnStartIndex, java.sql.Types.BLOB);
					pstmt.setNull(columnStartIndex+1, java.sql.Types.INTEGER);
		    	} else {
					try {
						pstmt.setBlob(columnStartIndex, new ByteArrayInputStream(((String)data[0]).getBytes("UTF-8")));
					} catch (UnsupportedEncodingException e) {
						throw new RuntimeException(e);
					}
					pstmt.setInt(columnStartIndex+1, (Integer)data[1]);
		    	}
			}
		};

		WikipediaAnalyzer analyzer = new WikipediaAnalyzer();
		WikiprepESAdb.getInstance().getConnection().setAutoCommit(false);

		System.out.println("Loading all article titles");
		Statement stmt = WikiprepESAdb.getInstance().getConnection().createStatement(
				java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		stmt.execute("SELECT id, title FROM article");
		ResultSet rs = stmt.getResultSet();
		int count = 0;
		while (rs.next()) {
			String s = tokenizeString(new String(rs.getBytes(2), "UTF-8"), analyzer);
			if (s != null && s.length() > 2){
				Map<Integer, Integer> arr = allSurfaceNames.get(s);
				if (arr == null){
					arr = new HashMap<>();
				}
				arr.put(rs.getInt(1), arr.get(rs.getInt(1)) != null ? arr.get(rs.getInt(1)) + 1 : 1);
				allSurfaceNames.put(s, arr);
			}
			if (count % 100000 == 0) {
				System.out.println("Loaded " + count + " (unique names: " + allSurfaceNames.size() + ")");
			}
			count++;
		}		
		rs.close();
		stmt.close();

		System.out.println("Loading all surface names");
		Statement stmt2 = WikiprepESAdb.getInstance().getConnection().createStatement(
				java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
		stmt2.setFetchSize(Integer.MIN_VALUE);
		stmt2.execute("SELECT target_id, anchor FROM anchor_text");
		ResultSet rs2 = stmt2.getResultSet();
		while (rs2.next()) {
			String s = tokenizeString(new String(rs2.getBytes(2), "UTF-8"), analyzer);
			if (s != null && s.length() > 2){
				if (s.length() > 250){
					continue;
				}
				Map<Integer,Integer> arr = allSurfaceNames.get(s);
				if (arr == null){
					arr = new HashMap<>();
				}
				arr.put(rs2.getInt(1), arr.get(rs2.getInt(1)) != null ? arr.get(rs2.getInt(1)) + 1 : 1);
				allSurfaceNames.put(s, arr);
			}
			if (count % 100000 == 0) {
				System.out.println("Loaded " + count + " (unique names: " + allSurfaceNames.size() + ")");
			}
			count++;
		}		
		rs2.close();
		stmt2.close();

		// Prepare tables
		System.out.println("Preparing tables...");
		stmt = WikiprepESAdb.getInstance().getConnection().createStatement();
		try{
			stmt.execute("DROP TABLE surface_names");
		}catch(Exception e) {
			// this is ok
		}
		stmt.execute("CREATE TABLE surface_names (name VARBINARY(256) NOT NULL, concept_id INT(10) NOT NULL)");
		stmt.close();

		// Do the updating
		c = 0;
		allSurfaceNames.forEach((name, ids) -> {
			ids.forEach((id,form_count)->{
				if (form_count < COUNT_THRESHHOLD) {
					return;
				}
		    	try {
		    		Object[] data = {name, new Integer(id)};
		    		bulkDbInserter.addRow(data);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
			if (c % 100000 == 0) {
				System.out.println("Updated " + c);
			}
			c++;
		});
		bulkDbInserter.finalize();
		analyzer.close();

		System.out.println("Creating Index...");
		stmt = WikiprepESAdb.getInstance().getConnection().createStatement();
		stmt.execute("ALTER TABLE surface_names ADD INDEX name_idx (name ASC)");
		stmt.close();

		WikiprepESAdb.getInstance().getConnection().commit();
		WikiprepESAdb.getInstance().getConnection().close();
		System.out.println("Done...");
	}

	public  static String tokenizeString(String str, WikipediaAnalyzer analyzer) {
        TokenStream ts = analyzer.tokenStream("contents",new StringReader(stripTitle(str)));
        
        StringBuffer sb = new StringBuffer();
        sb.append("$");
        try {
			ts.reset();
	        while (ts.incrementToken()) { 
	            TermAttribute t = ts.getAttribute(TermAttribute.class);
	            sb.append('_').append(t.term());
	        }
		} catch (Exception e) {
			System.out.println("Exception while processing \"" + str);
			e.printStackTrace();
		}
        return sb.length() > 0 ? sb.toString() : null;
	}
	
	public static String stripTitle(String title) {
		if (title.contains("(")) {
			title = title.substring(0, title.indexOf('('));
		}
		return title.trim();
	}
}
