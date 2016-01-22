package edu.wiki.concept;

import java.util.Arrays;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.util.InplaceSorts;

public class ArrayListConceptVectorOrderedIterator implements IConceptIterator{
	private int[] index;
	private double[] values;
	
	private int currentPos;
	
	public ArrayListConceptVectorOrderedIterator( int[] index, double[] values, int sz) {
		this.index = Arrays.copyOf(index, sz);
		this.values = Arrays.copyOf(values, sz);
		InplaceSorts.quicksort(values, index );
		reset();
	}
	
	public int getId() {
		return index[currentPos];
	}

	public double getValue() {
		return values[currentPos];
	}

	public boolean next() {
		currentPos--;
		if( currentPos >= 0 ) {
			return true;
		}
		else {
			return false;
		}
	}

	public void reset() {
		currentPos = index.length;
	}

}
