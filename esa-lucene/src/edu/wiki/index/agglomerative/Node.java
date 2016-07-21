package edu.wiki.index.agglomerative;

import java.util.function.Consumer;

public class Node<T> {
	protected Node<T> parent;
	protected Node<T> l,r;
	protected T point;
	protected double eval;
	protected int subTreeSize;
	
	public Node(T point, double eval){
		this.point = point;
		this.eval = eval;
		subTreeSize = 1;
	}
	
	public Node(Node<T> l, Node<T> r, T point, double eval){
		this.l = l;
		this.r = r;
		this.point = point;
		this.eval = eval;
		l.setParent(this);
		r.setParent(this);
		subTreeSize = l.getSubTreeSize() + r.getSubTreeSize();
	}
	
	public void setParent(Node<T> parent){
		this.parent = parent;
	}
	
	public boolean isLeaf(){
		return l == null && r == null;
	}
	
	public int getSubTreeSize(){
		return subTreeSize;
	}
	
	public T getPoint(){
		return point;
	}
	
	public double getEval(){
		return eval;
	}
	
	public void forEachLeaf(Consumer<Node<T>> consumer){
		if(isLeaf()){
			consumer.accept(this);
		}else{
			l.forEachLeaf(consumer);
			r.forEachLeaf(consumer);
		}
	}
}
