package edu.wiki.concept;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.util.MinHeapIntDouble;

public class ArrayListBestKOrderedIterator implements IConceptIterator{
	private int[] ids;
	private double[] values;
	private int current;
	 
	public ArrayListBestKOrderedIterator( int[] ids, double[] values, int sz,int nConcepts) {
		MinHeapIntDouble heap = new MinHeapIntDouble(nConcepts);
		for(int i = 0; i < ids.length; i++){
			if (i < nConcepts) {
				heap.insert(ids[i], values[i]);
			} else {
				heap.insertAtFront(ids[i], values[i]);
			}
		}
		
		this.values = new double[heap.size()];
		this.ids = new int[heap.size()];
		for (int i = heap.size() - 1; i >= 0; i--) {
			this.values[i] = heap.peekValue();
			this.ids[i] = heap.peekIndex();
			heap.remove();
		}
		reset();
	}
	
	public int getId() {
		return ids[current];
	}

	public double getValue() {
		return values[current];
	}

	public boolean next() {
		if (current >= ids.length - 1) {
			return false;
		}
		current++;
		return true;
	}

	public void reset() {
		current = -1;
	}
}
