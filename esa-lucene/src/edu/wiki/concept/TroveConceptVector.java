package edu.wiki.concept;

import java.io.Serializable;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntDoubleIterator;

public class TroveConceptVector implements IConceptVector, Serializable {

	private static final long serialVersionUID = 5228670885044409972L;

	private TIntDoubleHashMap valueMap;
	private int id;
	private String desc;
	
	public TroveConceptVector(int size ) {
		valueMap = new TIntDoubleHashMap(size);		
	}
	
	@Override
	public void add(int key, double d) {
		valueMap.adjustOrPutValue(key, d, d);
	}

	@Override
	public void add( IConceptVector v ) {
		IConceptIterator it = v.iterator();
		while( it.next() ) {
			add( it.getId(), it.getValue() );
		}
	}

	@Override
	public double get( int key ) {
		return valueMap.get( key );
	}

	@Override
	public IConceptIterator iterator() {
		return new TroveConceptVectorIterator();
	}

	@Override
	public IConceptIterator orderedIterator() {
		return new TroveConceptVectorOrderedIterator( valueMap );
	}
	
	public IConceptIterator bestKOrderedIterator(int nConcepts) {
		return new TroveBestKOrderedIterator(valueMap, nConcepts);
	}

	@Override
	public void set( int key, double d ) {
		if( d != 0 ) {
			valueMap.put( key, d );
		}
		else {
			valueMap.remove( key );
		}
	}

	@Override
	public int size() {
		return valueMap.size();
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

	@Override
	public double norm2() {
		double[] n = new double[1];
		n[0] = 0;
		valueMap.forEachValue((v) ->{
			n[0] += v * v;
			return true;
		});
		
		return Math.sqrt(n[0]);
	}
	
	private class TroveConceptVectorIterator implements IConceptIterator {

		TIntDoubleIterator valueIt;
		
		private TroveConceptVectorIterator() {
			reset();
		}
		
		@Override
		public int getId() {
			return valueIt.key();
		}

		@Override
		public double getValue() {
			return valueIt.value();
		}

		@Override
		public boolean next() {
			if( valueIt.hasNext() ) {
				valueIt.advance();
				return true;
			}
			return false;
		}

		@Override
		public void reset() {
			valueIt = valueMap.iterator();
		}
		
	}

	@Override
	public void multipty(double c) {
		// TODO Auto-generated method stub
		TIntDoubleIterator iter = valueMap.iterator();
		while(iter.hasNext()){
			iter.advance();
			iter.setValue(iter.value() / c);
		}
	}
}
