package edu.wiki.util;

public class Tuple<X,Y> {
  public final X x; 
  public final Y y; 
  public Tuple(X x, Y y) { 
    this.x = x; 
    this.y = y; 
  } 
  
  @Override
  public boolean equals(Object obj) {
	  if (!(obj instanceof Tuple)){
		  return false;
	  }
	  Tuple<?,?> t = (Tuple<?,?>)obj;
	  return x.equals(t.x) && y.equals(t.y);
  }
  
  @Override
  public int hashCode() {
	  return x.hashCode() * 257 + y.hashCode() * 137;
  }
  
  @Override
  public String toString() {
	  return new StringBuffer().append("<").append(x.toString()).append(",").append(y.toString()).append(">").toString();
  }
}
