package edu.wiki.util;

public class MinHeap {
    private int[] heapIndex;
    private int[] heapSource;
    private double[] heapValue;
    private int size;
    private int maxsize;
 
    private static final int FRONT = 1;
 
    public MinHeap(int maxsize)
    {
        this.maxsize = maxsize;
        this.size = 0;
        heapIndex = new int[this.maxsize + 1];
        heapSource = new int[this.maxsize + 1];
        heapValue = new double[this.maxsize + 1];
        heapIndex[0] = Integer.MIN_VALUE;
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

        int tmpSource = heapSource[fpos];
        heapSource[fpos] = heapSource[spos];
        heapSource[spos] = tmpSource;

        double tmpValue = heapValue[fpos];
        heapValue[fpos] = heapValue[spos];
        heapValue[spos] = tmpValue;
    }
 
    private void maxHeapify(int pos)
    {
        if (!isLeaf(pos))
        {
            if ( heapIndex[pos] > heapIndex[leftChild(pos)]  || heapIndex[pos] > heapIndex[rightChild(pos)])
            {
                if (heapIndex[leftChild(pos)] < heapIndex[rightChild(pos)])
                {
                    swap(pos, leftChild(pos));
                    maxHeapify(leftChild(pos));
                }else
                {
                    swap(pos, rightChild(pos));
                    maxHeapify(rightChild(pos));
                }
            }
        }
    }
 
    public void insert(int index, int source, double value)
    {
        heapIndex[++size] = index;
        heapSource[++size] = source;
        heapValue[++size] = value;
        int current = size;
 
        while(heapIndex[current] < heapIndex[parent(current)])
        {
            swap(current,parent(current));
            current = parent(current);
        }	
    }
 
    public void remove()
    {
        heapIndex[FRONT] = heapIndex[size--]; 
        heapSource[FRONT] = heapSource[size--]; 
        heapValue[FRONT] = heapValue[size--]; 
        maxHeapify(FRONT);
    }

    public int peekIndex() {
    	return getIndex(FRONT);
    }

    public int peekSource() {
    	return getSource(FRONT);
    }
    
    public double peekValue() {
    	return getValue(FRONT);
    }
    
    public int getIndex(int i) {
    	return heapIndex[i];
    }
    public int getSource(int i) {
    	return heapSource[i];
    }
    public double getValue(int i) {
    	return heapValue[i];
    }
    
    public int size() {
    	return size;
    }
}