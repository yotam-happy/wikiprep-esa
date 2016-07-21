package edu.wiki.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.CustomFilter;
import org.apache.lucene.analysis.CustomTokenizer;
import org.apache.lucene.analysis.LengthFilter;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardFilter;


public class WikipediaAnalyzer extends Analyzer {
	
	/** An unmodifiable set containing some common English words that are not usually useful
	  for searching.*/
	public final Set<?> ENGLISH_STOP_WORDS_SET;
	
	public WikipediaAnalyzer() {
		try{
			// read stop words
			FileInputStream fis = new FileInputStream(new File("config/stopwords.txt"));
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			ArrayList<String> stopWords = new ArrayList<String>(500);
			
			String line;
			
			while((line = br.readLine()) != null){
				line = line.trim();
				if(!line.equals("")){
					stopWords.add(line.trim());
				}
			}
			
			br.close();
			
			final CharArraySet stopSet = new CharArraySet(stopWords.size(), false);
			stopSet.addAll(stopWords);  
					
			ENGLISH_STOP_WORDS_SET = CharArraySet.unmodifiableSet(stopSet);
		}catch(IOException e){
			throw new RuntimeException(e);
		}
	}

    public TokenStream tokenStream(
        String fieldName, Reader reader) {

        // Tokenizer tokenizer = new LetterTokenizer(reader);
    	Tokenizer tokenizer = new CustomTokenizer(reader);

        TokenStream stream = new StandardFilter(tokenizer);
        stream = new LengthFilter(stream, 3, 100);
        stream = new LowerCaseFilter(stream);
        // stream = new StopFilter(true, stream, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
        stream = new StopFilter(true, stream, ENGLISH_STOP_WORDS_SET);
        stream = new CustomFilter(stream);
        stream = new PorterStemFilter(stream);
        stream = new PorterStemFilter(stream);
        stream = new PorterStemFilter(stream);

        return stream;
    }

    public TokenStream tokenStreamNoStemming(
            String fieldName, Reader reader) {

            // Tokenizer tokenizer = new LetterTokenizer(reader);
        	Tokenizer tokenizer = new CustomTokenizer(reader);

            TokenStream stream = new StandardFilter(tokenizer);
            stream = new LengthFilter(stream, 3, 100);
            stream = new LowerCaseFilter(stream);
            // stream = new StopFilter(true, stream, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
            stream = new StopFilter(true, stream, ENGLISH_STOP_WORDS_SET);
            stream = new CustomFilter(stream);

            return stream;
        }
}