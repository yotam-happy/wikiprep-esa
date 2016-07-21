package edu.wiki.api.concept;

import java.util.function.BiConsumer;



public interface IConceptVector {

	public double get(int key);
	
	public void add(int key, double d);

	public void set(int key, double d);
	
	public void add( IConceptVector v );
	
	public void multipty(double c);

	public IConceptIterator iterator();
	
	public int size();
	double norm2();
	
	public IConceptIterator orderedIterator();
	public IConceptIterator bestKOrderedIterator(int nConcepts);

	// meta data
	public void setId(int id);
	public int getId();
	public void setDesc(String desc);
	public String getDesc();
	
	default public void forEach(BiConsumer<Integer,Double> consumer){
		IConceptIterator it = iterator();
		while(it.next()){
			consumer.accept(it.getId(), it.getValue());
		}
	}

}
