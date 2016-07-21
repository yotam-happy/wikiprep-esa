package edu.wiki.index;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import edu.wiki.util.Tuple;
import edu.wiki.util.WikiprepESAdb;
import edu.wiki.util.counting.Counting;

public class MyIndexer {
	File tmp, tmpSorted; 
	
	int ngram = 2;
	
	MyIndexer() throws IOException{
		tmp = new File("myindexer.tmp");
		tmpSorted = new File("myindexer.sorted.tmp");
		System.out.println("Temporary file path: " + tmp.getAbsolutePath());
		System.out.println("Temporary sorted file path: " + tmp.getAbsolutePath());
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, SQLException{
		MyIndexer indexer = new MyIndexer();
		indexer.documentsToTermFile();
		indexer.sortTermFile();
//		indexer.readFile1();
		indexer.sortedTermFileToDB();
	}
	static final int MIN_COUNT = 3;
	static final int MIN_DOC_SIZE = 100;
	void documentsToTermFile() throws IOException {
		int[] nDocs = new int[1];
		
		Map<String, Double> termDf = new HashMap<>();
		System.out.println("Calculating term df");
		forEachWikiDocuments((id,txt)->{
			Map<String,Double> terms = tokenizeQuery(txt, ngram);
			if (terms.entrySet().stream().mapToDouble((e)->e.getValue()).sum() < MIN_DOC_SIZE){
				return;
			}
			nDocs[0]++;
			terms.forEach((term,count)->{
					Double d = termDf.get(term);
					termDf.put(term, d == null ? 1 : d + 1);
			});
		});
		
		System.out.println("Writing term tf-idf to file");
		FileWriter fw = new FileWriter(tmp);
		forEachWikiDocuments((id,txt)->{
			Map<String,Double> terms = tokenizeQuery(txt, ngram);
			
			if (terms.entrySet().stream().mapToDouble((e)->e.getValue()).sum() < MIN_DOC_SIZE){
				return;
			}

			Double tot = Math.sqrt(terms.entrySet().stream()
			.mapToDouble((e)->{
				if (termDf.get(e.getKey()) <= MIN_COUNT){
					return 0;
				}
				return (1 + Math.log(e.getValue())) * Math.log(nDocs[0] / termDf.get(e.getKey()));
			})
			.map((x)->Math.pow(x, 2)).sum());
			
			terms.forEach((term,count)->{
				if (termDf.get(term) <= MIN_COUNT){
					return;
				}
				double tfidf = (1 + Math.log(count)) * Math.log(nDocs[0] / termDf.get(term)) / tot;
				try{
					fw.write(term + "\t" + id + "\t" + tfidf + "\n");
				}catch(Exception e){
					try {
						fw.close();
					} catch (Exception e1) {
						throw new RuntimeException(e1);
					}
					throw new RuntimeException(e);
				}
			});
		});
		
		fw.close();
	}
	
	void sortTermFile() throws IOException, InterruptedException{
		SortFile sf = new SortFile(tmp.getAbsolutePath(), tmpSorted.getAbsolutePath());
		sf.sort();
	}
	
	void saveToDB(String curTerm, ArrayList<Tuple<Integer,Double>> vec) throws SQLException, IOException{
    	ByteArrayOutputStream baos = new ByteArrayOutputStream(100000);
    	DataOutputStream tmpdos = new DataOutputStream(baos);

    	tmpdos.writeInt(vec.size());
    	vec.forEach((t)->{
    		try{
    			tmpdos.writeInt(t.x);
    			tmpdos.writeFloat(t.y.floatValue());
    		}catch(Exception e){
    			throw new RuntimeException(e);
    		}
    	});
    	tmpdos.flush();

		pstmtWrite.setBlob(1, new ByteArrayInputStream(curTerm.getBytes("UTF-8")));
    	pstmtWrite.setBlob(2, new ByteArrayInputStream(baos.toByteArray()));
    	pstmtWrite.execute();
	}
	PreparedStatement pstmtWrite = null;
	String tableName = "idx" + (ngram > 1 ? "_" + ngram : "");
	String strVectorInsert = "INSERT INTO " + tableName + " (term,vector) VALUES (?,?)";
	
	void sortedTermFileToDB() throws IOException, SQLException{
		Statement stmt = WikiprepESAdb.getInstance().getConnection().createStatement();
		stmt.execute("DROP TABLE IF EXISTS " + tableName);
		stmt.execute("CREATE TABLE " + tableName + " (" +
				"term VARBINARY(255)," +
				"vector MEDIUMBLOB " +
				") DEFAULT CHARSET=binary");
		stmt.close();

		WikiprepESAdb.getInstance().getConnection().setAutoCommit(false);

		pstmtWrite = 
				WikiprepESAdb.getInstance().getConnection().prepareStatement(strVectorInsert);

		BufferedReader br = new BufferedReader(new FileReader(tmpSorted));
		System.out.println("Writing to DB...");
		String line = null;
		String curTerm = null;
		ArrayList<Tuple<Integer,Double>> vec = null;
		Counting c = new Counting(10);
		Set<String> loaded = new HashSet<>();
		while ((line = br.readLine()) != null) {
			String[] parts = line.split("\t");
			if(parts[0].contains("?")){
				// these have unknown chars
				continue;
			}
			if (!parts[0].equals(curTerm)){
				 if (vec != null){
					 // save to db
					 if (loaded.contains(curTerm)){
						 System.out.println("duplicate: " + curTerm);
					 }
					 loaded.add(curTerm);
					 saveToDB(curTerm, vec);
					 c.addOne();
					 if(c.count() % 10000 == 0){
						 // commit every 100trm 00 vectors cous larg commits might fail
						 WikiprepESAdb.getInstance().getConnection().commit();
					 }
				 }

				 vec = new ArrayList<>();
				 curTerm = parts[0];
			}
			vec.add(new Tuple<>(Integer.valueOf(parts[1]), Double.valueOf(parts[2])));
		}

		br.close();
		stmt.close();
		WikiprepESAdb.getInstance().getConnection().setAutoCommit(false);
		
		stmt = WikiprepESAdb.getInstance().getConnection().createStatement();
		stmt.execute("ALTER TABLE " + tableName + " " +
				"ADD PRIMARY KEY (term)");
		stmt.close();
	}

	void forEachWikiDocuments(BiConsumer<Integer, String> consumer){
		WikiprepESAdb.getInstance().forEachResult("SELECT old_id, old_text FROM text", (rs)->{
			try {
				String text = new String(rs.getBytes(2), "UTF-8");
				int id = rs.getInt(1);
				consumer.accept(id, text);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}
	
	WikipediaAnalyzer analyzer = new WikipediaAnalyzer();

	// fast trimming of non alphanumeric chars (regex very slow)
	String cleanToken(String s){
		boolean gotSomething = false;
		for(int i = 0; i < s.length(); i++){
			if(Character.isLetter(s.charAt(i)) || Character.isDigit(s.charAt(i))){
				if(i > 0){
					s = s.substring(i, s.length());
				}
				gotSomething = true;
				break;
			}
		}
		if (!gotSomething){
			return null;
		}
		for(int i = s.length() - 1; i >= 0; i--){
			if(Character.isLetter(s.charAt(i)) || Character.isDigit(s.charAt(i))){
				if(i > 0){
					s = s.substring(0, i+1);
				}
				break;
			}
		}
		return s;
	}
	
	boolean filterToken(String t){
		for(int i = 0; i < t.length(); i++){
			if(t.charAt(i) >= 'a' && t.charAt(i) <= 'z'){
				return false;
			}
		}
		return true;
	}
	
	Map<String,Double> tokenizeQuery(String query, int ngram){
		String[] grams = new String[ngram]; 
		Map<String,Double> terms = new HashMap<>();
        TokenStream ts = analyzer.tokenStream("contents",new StringReader(query));
        try {
			ts.reset();
	        while (ts.incrementToken()) { 
	            TermAttribute ta = ts.getAttribute(TermAttribute.class);
	            String t = ta.term();
	            t = cleanToken(t);
	            if (t == null || t.isEmpty() || filterToken(t)){
	            	continue;
	            }
	            for(int i = 0; i < ngram - 1; i++){
	            	grams[i] = grams[i+1];
	            }
	            grams[ngram-1] = t;
	            if(grams[0] != null){
	            	String g = "";
	            	for(int i = 0; i < ngram; i++){
	            		g += (i != 0 ? "_" : "") + grams[i];
	            	}
		            Double d = terms.get(g);
		            terms.put(g, d == null ? 1 : d + 1);
	            }
	        }
	                
	        ts.end();
	        ts.close();
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
		return terms;
	}
}