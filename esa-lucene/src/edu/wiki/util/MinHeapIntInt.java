package edu.wiki.util;

public class MinHeapIntInt {
    private int[] heapIndex;
    private int[] heapValue;
    private int size;
    private int maxsize;
 
    private static final int FRONT = 1;
 
    public MinHeapIntInt(int maxsize)
    {
        this.maxsize = maxsize;
        this.size = 0;
        heapIndex = new int[this.maxsize + 1];
        heapValue = new int[this.maxsize + 1];
        heapValue[0] = Integer.MIN_VALUE;
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
 
    private void swap(int fpos,int spos)
    {
        int tmpIndex = heapIndex[fpos];
        heapIndex[fpos] = heapIndex[spos];
        heapIndex[spos] = tmpIndex;

        int tmpValue = heapValue[fpos];
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
 
    public void insert(int index, int value)
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
    
    public void insertAtFront(int index, int value) {
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

    public int peekValue() {
    	return getValue(FRONT);
    }
    
    public int getIndex(int i) {
    	return heapIndex[i];
    }
    public int getValue(int i) {
    	return heapValue[i];
    }
    
    public int size() {
    	return size;
    }
}
