package edu.wiki.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import edu.wiki.util.WikiprepESAdb;

public class RandomTermFileGenerator {
	File tmp, tmpRandom; 
	
	int ngram = 1;
	
	public static final String DELIM = " "; 
	
	RandomTermFileGenerator() throws IOException{
		tmp = new File("myindexer.tmp");
		tmpRandom = new File("myindexer.random.tmp");
		System.out.println("Temporary file path: " + tmp.getAbsolutePath());
		System.out.println("Temporary random file path: " + tmp.getAbsolutePath());
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, SQLException{
		RandomTermFileGenerator indexer = new RandomTermFileGenerator();
//		indexer.documentsToTermFile();
//		indexer.sortTermFile();
//		indexer.readFile1();
		indexer.readFile2();
	}

	static final int MIN_COUNT = 3;
	static final int MIN_DOC_SIZE = 100;
	
	@SuppressWarnings("resource")
	void documentsToTermFile() throws IOException {

		Map<String, Integer> termDf = new HashMap<>();
		Map<String, Integer> termCount = new HashMap<>();
		Map<Integer,Integer> docLen = new HashMap<>();
		System.out.println("Calculating term df");
		forEachWikiDocuments((id,txt)->{
			Map<String,Double> terms = tokenizeQuery(txt, ngram);

			// filter short docs
			if (terms.entrySet().stream().mapToDouble((e)->e.getValue()).sum() < MIN_DOC_SIZE){
				return;
			}
			docLen.put(id, terms.values().stream().mapToInt((v)->v.intValue()).sum());
			terms.forEach((term,count)->{
					Integer d = termDf.get(term);
					termDf.put(term, d == null ? 1 : d + 1);
					Integer dd = termCount.get(term);
					termCount.put(term, dd == null ? count.intValue() : dd + count.intValue());
			});
		});
		
		FileWriter fw2 = new FileWriter(tmp + ".document.freq");
		docLen.forEach((id,freq)->{
			try {
				fw2.write(id + DELIM + freq + "\n");
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
		fw2.close();
		FileWriter fw3 = new FileWriter(tmp + ".term.freq");
		termCount.forEach((term,freq)->{
			try {
				fw3.write(term + DELIM + freq + "\n");
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
		fw3.close();

		System.out.println("Writing terms to file");
		FileWriter fw = new FileWriter(tmp);
		forEachWikiDocuments((id,txt)->{
			Map<String,Double> terms = tokenizeQuery(txt, ngram);
			
			// filter short docs
			if (terms.entrySet().stream().mapToDouble((e)->e.getValue()).sum() < MIN_DOC_SIZE){
				return;
			}

			terms.forEach((term,count)->{
				// filter infrequent terms
				if (termDf.get(term) <= MIN_COUNT){
					return;
				}
				
				try{
					for(int i = 0; i < count; i++){
						fw.write(term + DELIM + id + "\n");
					}
				}catch(Exception e){
					throw new RuntimeException(e);
				}
			});
		});
		
		fw.close();
	}
	
	void sortTermFile() throws IOException, InterruptedException{
		SortFile sf = new SortFile(tmp.getAbsolutePath(), tmpRandom.getAbsolutePath(), true);
		sf.sort();
	}
	
	void forEachWikiDocuments(BiConsumer<Integer, String> consumer){
		forEachWikiDocuments(0,consumer);
	}
	void forEachWikiDocuments(int limit, BiConsumer<Integer, String> consumer){
		WikiprepESAdb.getInstance().forEachResult("SELECT old_id, old_text FROM text" + (limit > 0 ? " LIMIT " + limit : ""), (rs)->{
			try {
				String text = new String(rs.getBytes(2), "UTF-8");
				int id = rs.getInt(1);
				consumer.accept(id, text);
				
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}

	
	// TODO: These shouldn't be here!!!!
	
	
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
		Map<String,Double> terms = new HashMap<>();
        TokenStream ts = analyzer.tokenStreamNoStemming("contents",new StringReader(query));
        try {
			ts.reset();
	        while (ts.incrementToken()) { 
	            TermAttribute ta = ts.getAttribute(TermAttribute.class);
	            String t = ta.term();
	            t = cleanToken(t);
	            if (t == null || t.isEmpty() || filterToken(t)){
	            	continue;
	            }
	            Double d = terms.get(t);
	            terms.put(t, d == null ? 1 : d + 1);
	        }
	                
	        ts.end();
	        ts.close();
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
		return terms;
	}
	
	private void readFile1() throws IOException {
		FileInputStream fis = new FileInputStream(tmp);
	 
		//Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
	 
		String line = null;
		int c = 0;
		while ((line = br.readLine()) != null && c < 1000) {
			System.out.println(line);
			c++;
		}
	 
		br.close();
	}	
	private void readFile2() throws IOException {
		FileInputStream fis = new FileInputStream("C:/Python/yoavgo-word2vecf-90e299816bcd/dim200vecs");
	 
		//Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
	 
		String line = null;
		int c = 0;
		char[] cbuf = new char[100];
		while ((-1 != br.read(cbuf))  && c < 10) {
			System.out.println(cbuf);
			c++;
		}
	 
		br.close();
	}	
}
