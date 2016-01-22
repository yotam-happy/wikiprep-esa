package edu.wiki.concept;

import java.util.Arrays;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.api.concept.IConceptVectorData;
import edu.wiki.util.MinHeapIntInt;
import gnu.trove.TDoubleProcedure;

/**
 * This implementation is good for very fast similarity measures.
 * Not so fast for anything else
 * @author yotamesh
 *
 */
public class ArrayListConceptVector implements IConceptVector{
	private int[] ids;
	private double[] scores;
	private int size;
	
	private int id;
	private String desc;
	
	public ArrayListConceptVector(int capacity ) {
		this.size = 0;
		ids = new int[capacity];
		scores = new double[capacity];
		for(int i = 0; i < capacity; i++){
			ids[i] = -1;
		}
	}
	
	public ArrayListConceptVector(IConceptVector v) {
		size = v.count();
		ids = new int[size];
		scores = new double[size];
		
		int c = 0;
		IConceptIterator it = v.iterator();
		while(it.next()) {
			ids[c] = it.getId();
			c++;
		}
		Arrays.sort(ids);
		for (int i = 0; i < size; i++){
			scores[i] = v.get(ids[i]);
		}
	}
	
	@Override
	public void setId(int id){
		this.id = id;
	}
	
	@Override
	public int getId(){
		return id;
	}

	@Override
	public void setDesc(String desc){
		this.desc = desc;
	}
	
	@Override
	public String getDesc(){
		return desc;
	}
	
	public void normalizeLength() {
		double norm = this.getData().getNorm2();
		this.multipty((float)(1.0 / norm));
	}
	
	private void maintainCapacity(int newSize) {
		if (newSize > ids.length) {
			ids = Arrays.copyOf(ids, ids.length + 1 + ids.length / 4);
			scores = Arrays.copyOf(scores, ids.length + 1 + ids.length / 4);
		}
	}
	/**
	 * This is very inefficient in this implementation.
	 */
	@Override
	public void add(int key, double d) {
		int pos = Arrays.binarySearch(ids, 0, size, key);
		if (pos >= 0) {
			scores[pos] += d;
		} else {
			pos = -pos - 1; // get insertion point
			maintainCapacity(size + 1);
			for (int i = size - 1; i >= pos; i--) {
				ids[i + 1] = ids[i];
				scores[i + 1] = scores[i];
			}
			ids[pos] = key;
			scores[pos] = d;
			size++;
		}
	}

	@Override
	public void add(IConceptVector v) {
		IConceptIterator it = v.iterator();
		while( it.next() ) {
			add(it.getId(), it.getValue());
		}
	}

	public static ArrayListConceptVector merge(ArrayListConceptVector[] vecs) {
		if(vecs.length == 0) {
			return null;
		}
		ArrayListConceptVector res = new ArrayListConceptVector(vecs[0].size);
		
		// init min heap
		MinHeapIntInt heap = new MinHeapIntInt(vecs.length);
		int[] indexes = new int[vecs.length];
		for(int i = 0; i < vecs.length; i++) {
			if (vecs[i].size > 0){
				indexes[i] = 0;
				heap.insert(i, vecs[i].ids[indexes[i]]);
			}
		}
		
		int lastId = -1;
		double score = 0;
		while(heap.size() > 0) {
			int curId = heap.peekValue();
			int curVec =heap.peekIndex();

			if (lastId != curId) {
				if (lastId != -1) {
					res.maintainCapacity(res.size+1);
					res.ids[res.size] = lastId;
					res.scores[res.size] = score;
					res.size++;
				}
				lastId = curId;
				score = vecs[curVec].scores[indexes[curVec]];
			} else {
				score += vecs[curVec].scores[indexes[curVec]];
			}

			indexes[curVec]++;
			heap.remove();
			
			if (indexes[curVec] < vecs[curVec].size){
				heap.insert(curVec, vecs[curVec].ids[indexes[curVec]]);
			}
		}
		return res;
	}
	
	@Override
	public int count() {
		return size;
	}

	@Override
	public double get( int key ) {
		int i = Arrays.binarySearch(ids, 0, size, key);
		return i >= 0 ? scores[i] : 0.0;
	}

	@Override
	public IConceptVectorData getData() {
		return new ArrayListConceptVectorData();
	}

	@Override
	public IConceptIterator iterator() {
		return new ArrayListConceptVectorIterator();
	}

	@Override
	public IConceptIterator orderedIterator() {
		return new ArrayListConceptVectorOrderedIterator(ids, scores, size);
	}
	
	public IConceptIterator bestKOrderedIterator(int nConcepts) {
		return new ArrayListBestKOrderedIterator(ids, scores, size, nConcepts);
	}

	@Override
	public void set( int key, double d ) {
		if (d == 0) {
			// remove
			int pos = Arrays.binarySearch(ids, 0, size, key);
			if (pos >= 0) {
				for (int i = pos; i < size - 1; i++){
					ids[i] = ids[i+1];
					scores[i] = scores[i+1];
				}
				size--;
			}
		} else {
			int pos = Arrays.binarySearch(ids, 0, size, key);
			if (pos >= 0) {
				scores[pos] = d;
			} else {
				add(key, d);
			}
		}
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public void multipty(Float c) {
		for (int i = 0; i < size; i++) {
			scores[i] *= c;
		}
	}

	private class ArrayListConceptVectorData implements IConceptVectorData {
		
		public int getConceptCount() {
			return size;
		}
		
		public double getNorm1() {
			NormProcedure n1 = new NormProcedure( 1 );
			for (int i = 0; i < size; i++) {
				n1.execute(scores[i]);
			}
			return n1.getNorm();
		}
		
		public double getNorm2() {
			NormProcedure n2 = new NormProcedure( 2 );
			for (int i = 0; i < size; i++) {
				n2.execute(scores[i]);
			}
			return n2.getNorm();
		}
		
		private class NormProcedure implements TDoubleProcedure {

			int p;
			double sum;
			
			private NormProcedure( int p ) {
				this.p = p;
				sum = 0;
			}
			
			@Override
			public boolean execute( double value ) {
				if( p == 1 ) {
					sum += value;
				}
				else {
					sum += Math.pow( value, p );
				}
				return true;
			}
			
			private double getNorm() {
				return Math.pow( sum, 1.0 / (double)p );
			}
		}
	}
	
	private class ArrayListConceptVectorIterator implements IConceptIterator {

		int c = -1;
		
		private ArrayListConceptVectorIterator() {
			reset();
		}
		
		@Override
		public int getId() {
			return ids[c];
		}

		@Override
		public double getValue() {
			return scores[c];
		}

		@Override
		public boolean next() {
			c++;
			if( c < size) {
				return true;
			}
			return false;
		}

		@Override
		public void reset() {
			c = -1;
		}
		
	}
}
