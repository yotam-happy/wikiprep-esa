package edu.wiki.api.concept;



public interface IConceptVector {

	public double get(int key);
	
	public void add(int key, double d);

	public void set(int key, double d);
	
	public void add( IConceptVector v );
	
	public void multipty(Float c);

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

}
