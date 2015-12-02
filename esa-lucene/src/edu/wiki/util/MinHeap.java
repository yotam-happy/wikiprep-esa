package edu.wiki.util;

public class MinHeap {
    private int[] heapIndex;
    private double[] heapValue;
    private int size;
    private int maxsize;
 
    private static final int FRONT = 1;
 
    public MinHeap(int maxsize)
    {
        this.maxsize = maxsize;
        this.size = 0;
        heapIndex = new int[this.maxsize + 1];
        heapValue = new double[this.maxsize + 1];
        heapValue[0] = Double.MIN_VALUE;
    }
 
    private static int parent(int pos)
    {
        return pos / 2;
    }
 
    private static int leftChild(int pos)
    {
        return (2 * pos);
    }
 
    private static int rightChild(int pos)
    {
        return (2 * pos) + 1;
    }
 
    private boolean isLeaf(int pos)
    {
        if (pos >=  (size / 2)  &&  pos <= size)
        {
            return true;
        }
        return false;
    }
 
    private void swap(int fpos,int spos)
    {
        int tmpIndex = heapIndex[fpos];
        heapIndex[fpos] = heapIndex[spos];
        heapIndex[spos] = tmpIndex;

        double tmpValue = heapValue[fpos];
        heapValue[fpos] = heapValue[spos];
        heapValue[spos] = tmpValue;
    }
 
    private void heapify(int pos)
    {
        if (pos <= size)
        {
            if ( (leftChild(pos) <= size && heapValue[pos] > heapValue[leftChild(pos)])  || 
            		(rightChild(pos) <= size && heapValue[pos] > heapValue[rightChild(pos)]))
            {
                if ((!(rightChild(pos) <= size)) || heapValue[leftChild(pos)] < heapValue[rightChild(pos)])
                {
                    swap(pos, leftChild(pos));
                    heapify(leftChild(pos));
                }else
                {
                    swap(pos, rightChild(pos));
                    heapify(rightChild(pos));
                }
            }
        }
    }
 
    public void insert(int index, double value)
    {
    	size++;
        heapIndex[size] = index;
        heapValue[size] = value;
        int current = size;
 
        while(heapValue[current] < heapValue[parent(current)])
        {
            swap(current,parent(current));
            current = parent(current);
        }	
    }
 
    public void remove()
    {
        heapIndex[FRONT] = heapIndex[size]; 
        heapValue[FRONT] = heapValue[size];
        size--;
        heapify(FRONT);
    }
    
    public void insertAtFront(int index, double value) {
    	if (value <= heapValue[FRONT]) {
    		return;
    	}
    	heapIndex[FRONT] = index;
    	heapValue[FRONT] = value;
    	heapify(FRONT);
    }

    public int peekIndex() {
    	return getIndex(FRONT);
    }

    public double peekValue() {
    	return getValue(FRONT);
    }
    
    public int getIndex(int i) {
    	return heapIndex[i];
    }
    public double getValue(int i) {
    	return heapValue[i];
    }
    
    public int size() {
    	return size;
    }
}