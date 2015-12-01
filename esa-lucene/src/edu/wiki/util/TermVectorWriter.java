package edu.wiki.util;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public class TermVectorWriter {
	private TermVectorWriter() {
	}
	
	public static void intoDataOutputStream(Map<Integer, Double> terms, DataOutputStream dos) {
		try{
			dos.writeInt(terms.size());
		}catch(IOException e) {
			throw new RuntimeException(e);
		}
		
		terms.entrySet().stream()
			.sorted((e1,e2)->e1.getKey().compareTo(e2.getKey()))
			.forEachOrdered((e)-> {
				try{
		    		dos.writeInt(e.getKey());
		    		dos.writeDouble(e.getValue());
				}catch(IOException e2) {
					throw new RuntimeException(e2);
				}
			});
	}
}
