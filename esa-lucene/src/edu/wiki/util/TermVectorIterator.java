package edu.wiki.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Stream structure:
 * 4 bytes: int - length of array
 * 4 byte (doc) - 8 byte (tfidf) pairs
 */
public class TermVectorIterator {
    private ByteArrayInputStream bais;
    private DataInputStream dis;
    private int vectorLen;
    private int next = 0;
    private int currConceptId;
    private float currConceptScore;
    
    public TermVectorIterator(byte[] vector) {
    	bais = new ByteArrayInputStream(vector);
    	dis = new DataInputStream(bais);
    	try {
			vectorLen = dis.readInt();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
    
    public boolean next() {
    	if (next >= vectorLen) {
    		return false;
    	}
    	try {
			currConceptId = dis.readInt();
	    	currConceptScore = dis.readFloat();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    	next++;
    	return true;
    }
    
    public int getConceptId() {
    	return currConceptId;
    }
    
    public float getConceptScore() {
    	return currConceptScore;
    }
    
    public int getVectorLen() {
    	return vectorLen;
    }
}
