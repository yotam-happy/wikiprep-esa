/**
 * This file is part of the Java Machine Learning Library
 * 
 * The Java Machine Learning Library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * The Java Machine Learning Library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with the Java Machine Learning Library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 * 
 * Copyright (c) 2006-2012, Thomas Abeel
 * 
 * Project: http://java-ml.sourceforge.net/
 * 
 */
package my.net.sf.javaml.clustering.mcl;

import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntDoubleIterator;

/**
 * SparseVector represents a sparse vector.
 * <p>
 * Conventions: except for the inherited methods and normalise(double),
 * operations leave <tt>this</tt> ummodified (immutable) if there is a return
 * value. Within operations, no pruning of values close to zero is done. Pruning
 * can be controlled via the prune() method.
 *
 * @author gregor :: arbylon . net
 */
public class SparseVector extends TIntDoubleHashMap {

    
    /**
     * 
     */
    private static final long serialVersionUID = 8101876335024188425L;
    private int length = 0;

    /**
     * create empty vector
     */
    public SparseVector() {
        super();
    }

    /**
     * create empty vector with length
     */
    public SparseVector(int i) {
        this();
        length = i;
    }

    /**
     * create vector from dense vector
     *
     * @param x
     */
    public SparseVector(double[] x) {
        this(x.length);
        for (int i = 0; i < x.length; i++) {
            if (x[i] != 0) {
                put(i, x[i]);
            }
        }
    }

    /**
     * copy constructor
     *
     * @param v
     */
    public SparseVector(SparseVector v) {
        super(v);
        this.length = v.length;
    }

    /**
     * get ensures it returns 0 for empty hash values or if index exceeds
     * length.
     *
     * @param key
     * @return val
     */
    @Override
    public double get(int key) {
        Double b = super.get(key);
        if (b == null) {
            return 0.;
        }
        return b;
    }

    /**
     * put increases the matrix size if the index exceeds the current size.
     *
     * @param key
     * @param value
     * @return
     */
    @Override
    public double put(int key, double value) {
        length = Math.max(length, key + 1);
        if (value == 0) {
            return remove(key);
        }
        return super.put(key, value);
    }

    /**
     * normalises the vector to 1.
     */
    public void normalise() {
        double invsum = 1. / sum();
        forEachKey((i)->{mult(i, invsum);return true;});
    }

    /**
     * normalises the vector to newsum
     *
     * @param the value to which the element sum
     * @return the old element sum
     */
    public double normalise(double newsum) {
        double sum = sum();
        double invsum = newsum / sum;
        forEachKey((i)->{mult(i, invsum);return true;});
        return sum;
    }

    /**
     * sum of the elements
     *
     * @return
     */
    private double sum() {
        double sum = 0;

        TIntDoubleIterator it = iterator();
        while(it.hasNext()){
        	it.advance();
        	sum += it.value();
        }
        return sum;
    }

    /**
     * power sum of the elements
     *
     * @return
     */
    public double sum(double s) {
        double sum = 0;
        TIntDoubleIterator it = iterator();
        while(it.hasNext()){
        	it.advance();
            sum += Math.pow(it.value(), s);
        }
        return sum;
    }

    /**
     * mutable add
     *
     * @param v
     */
    public void add(SparseVector v) {
    	v.forEachEntry((k,s)->{add(k,s);return true;});
    }

    /**
     * mutable mult
     *
     * @param i index
     * @param a value
     */
    public void mult(int i, double a) {
        Double c = get(i);
        c *= a;
        put(i, c);
    }

    /**
     * mutable factorisation
     *
     * @param a
     */
    public void factor(double a) {
        SparseVector s = copy();
        TIntDoubleIterator it = iterator();
        while(it.hasNext()){
        	it.advance();
        	s.mult(it.key(), a);
        }
    }

    /**
     * immutable scalar product
     *
     * @param v
     * @return scalar product
     */
    public double times(SparseVector v) {
        double sum = 0;

        TIntDoubleIterator it = iterator();
        while(it.hasNext()){
        	it.advance();
        	sum += it.value() * v.get(it.key());
        }
        return sum;
    }

    /**
     * mutable Hadamard product (elementwise multiplication)
     *
     * @param v
     */
    public void hadamardProduct(SparseVector v) {
        TIntDoubleIterator it = iterator();
        while(it.hasNext()){
        	it.advance();
        	it.setValue(it.value() * v.get(it.key()));
        }
    }

    /**
     * mutable Hadamard power
     *
     * @param s
     */
    public void hadamardPower(double s) {
        TIntDoubleIterator it = iterator();
        while(it.hasNext()){
        	it.advance();
        	it.setValue(Math.pow(it.value(), s));
        }
    }

    /**
     * mutable add
     *
     * @param i
     * @param a
     */
    public void add(int i, double a) {
        length = Math.max(length, i + 1);
        double c = get(i);
        c += a;
        put(i, c);
    }

    /**
     * get the length of the vector
     *
     * @return
     */
    public final int getLength() {
        return length;
    }

    /**
     * set the new length of the vector (regardless of the maximum index).
     *
     * @param length
     */
    public final void setLength(int length) {
        this.length = length;
    }

    /**
     * copy the contents of the sparse vector
     *
     * @return
     */
    public SparseVector copy() {
        return new SparseVector(this);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        TIntDoubleIterator it = iterator();
        while(it.hasNext()){
        	it.advance();
            sb.append(it.key()).append("->").append(it.value()).append(", ");
        }
        return sb.toString();
    }

    
    /**
     * create string representation of dense equivalent.
     *
     * @return
     */
//    public String toStringDense() {
//        return Vectors.print(getDense());
//    }

    /**
     * get dense represenation
     *
     * @return
     */
//    public double[] getDense() {
//    	// This is obviously buggy! makes no sense
//        double[] a = new double[length];
//        TIntDoubleIterator it = iterator();
//        while(it.hasNext()){
//        	it.advance();
//            a[it.key()] = it.value();
//        }
//        return a;
//    }

    
    /**
     * maximum element value
     *
     * @return
     */
    public double max() {
        double max = 0;
        TIntDoubleIterator it = iterator();
        while(it.hasNext()){
        	it.advance();
        	max = Math.max(it.value(), max);
        }
        return max;
    }

    /**
     * exponential sum, i.e., sum (elements^p)
     *
     * @param p
     * @return
     */
    public double expSum(int p) {
        double sum = 0;
        TIntDoubleIterator it = iterator();
        while(it.hasNext()){
        	it.advance();
            sum += Math.pow(it.value(), p);
        }
        return sum;
    }

    /**
     * remove all elements whose magnitude is < threshold
     *
     * @param threshold
     */
    public void prune(double threshold) {
        TIntDoubleIterator it = iterator();
        while(it.hasNext()){
        	it.advance();
            if (Math.abs(it.value()) < threshold) {
                it.remove();
            }
        }
    }
}
