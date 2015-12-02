package edu.wiki.concept;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.util.MinHeap;
import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntDoubleIterator;

public class TroveBestKOrderedIterator implements IConceptIterator{
	private int[] ids;
	private double[] values;
	private int current;
	 
	public TroveBestKOrderedIterator( TIntDoubleHashMap valueMap, int nConcepts ) {
		TIntDoubleIterator iter = valueMap.iterator();
		
		MinHeap heap = new MinHeap(nConcepts);
		int c = 0;
		while (iter.hasNext()) {
			iter.advance();
			if (c < nConcepts) {
				heap.insert(iter.key(), iter.value());
			} else {
				heap.insertAtFront(iter.key(), iter.value());
			}
			c++;
		}
		
		values = new double[heap.size()];
		ids = new int[heap.size()];
		for (int i = heap.size() - 1; i >= 0; i--) {
			values[i] = heap.peekValue();
			ids[i] = heap.peekIndex();
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
