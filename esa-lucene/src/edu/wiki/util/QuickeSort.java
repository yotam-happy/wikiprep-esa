package edu.wiki.util;

public class QuickeSort {
	public static void quicksort(double[] A, int[] B){
        int i,j,left = 0,right = A.length - 1,stack_pointer = -1;
        int[] stack = new int[128];
        double swap,temp;
        
        int swapI, tempI;
        
        while(true){
            if(right - left <= 7){
                for(j=left+1;j<=right;j++){
                    swap = A[j];
                    swapI = B[j];
                    i = j-1;
                    while(i>=left && A[i]> swap) {
                        A[i+1] = A[i];
                        B[i+1] = B[i];
                        i--;
                    }
                    A[i+1] = swap;
                    B[i+1] = swapI;
                }
                if(stack_pointer == -1)
                    break;
                right = stack[stack_pointer--];
                left = stack[stack_pointer--];
            }else{
                int median = (left + right) >> 1;
                i = left + 1;
                j = right;
                swap = A[median]; A[median] = A[i]; A[i] = swap;
                swapI = B[median]; B[median] = B[i]; B[i] = swapI;
                /* make sure: c[left] <= c[left+1] <= c[right] */
                if(A[left] > A[right]){
                    swap = A[left]; A[left] = A[right]; A[right] = swap;
                    swapI = B[left]; B[left] = B[right]; B[right] = swapI;
                }if(A[i] > A[right]){
                    swap = A[i]; A[i] = A[right]; A[right] = swap;
                    swapI = B[i]; B[i] = B[right]; B[right] = swapI;
                }if(A[left] > A[i]){
                    swap = A[left]; A[left] = A[i]; A[i] = swap;
                    swapI = B[left]; B[left] = B[i]; B[i] = swapI;
                }
                temp = A[i];
                tempI = B[i];
                while(true){
                    do i++; while(A[i] < temp);
                    do j--; while(A[j] > temp);
                    if(j < i)
                        break;
                    swap = A[i]; A[i] = A[j]; A[j] = swap;
                    swapI = B[i]; B[i] = B[j]; B[j] = swapI;
                }
                A[left + 1] = A[j];
                B[left + 1] = B[j];
                A[j] = temp;
                B[j] = tempI;
                if(right-i+1 >= j-left){
                    stack[++stack_pointer] = i;
                    stack[++stack_pointer] = right;
                    right = j-1;
                }else{
                    stack[++stack_pointer] = left;
                    stack[++stack_pointer] = j-1;
                    left = i;
                }
            }
        }
    }	

	public static void quicksort(int[] A, double[] B){
        int i,j,left = 0,right = A.length - 1,stack_pointer = -1;
        int[] stack = new int[128];
        double swap,temp;
        
        int swapI, tempI;
        
        while(true){
            if(right - left <= 7){
                for(j=left+1;j<=right;j++){
                    swapI = A[j];
                    swap = B[j];
                    i = j-1;
                    while(i>=left && A[i]> swapI) {
                        A[i+1] = A[i];
                        B[i+1] = B[i];
                        i--;
                    }
                    A[i+1] = swapI;
                    B[i+1] = swap;
                }
                if(stack_pointer == -1)
                    break;
                right = stack[stack_pointer--];
                left = stack[stack_pointer--];
            }else{
                int median = (left + right) >> 1;
                i = left + 1;
                j = right;
                swapI = A[median]; A[median] = A[i]; A[i] = swapI;
                swap = B[median]; B[median] = B[i]; B[i] = swap;
                /* make sure: c[left] <= c[left+1] <= c[right] */
                if(A[left] > A[right]){
                    swapI = A[left]; A[left] = A[right]; A[right] = swapI;
                    swap = B[left]; B[left] = B[right]; B[right] = swap;
                }if(A[i] > A[right]){
                    swapI = A[i]; A[i] = A[right]; A[right] = swapI;
                    swap = B[i]; B[i] = B[right]; B[right] = swap;
                }if(A[left] > A[i]){
                    swapI = A[left]; A[left] = A[i]; A[i] = swapI;
                    swap = B[left]; B[left] = B[i]; B[i] = swap;
                }
                tempI = A[i];
                temp = B[i];
                while(true){
                    do i++; while(A[i] < tempI);
                    do j--; while(A[j] > tempI);
                    if(j < i)
                        break;
                    swapI = A[i]; A[i] = A[j]; A[j] = swapI;
                    swap = B[i]; B[i] = B[j]; B[j] = swap;
                }
                A[left + 1] = A[j];
                B[left + 1] = B[j];
                A[j] = tempI;
                B[j] = temp;
                if(right-i+1 >= j-left){
                    stack[++stack_pointer] = i;
                    stack[++stack_pointer] = right;
                    right = j-1;
                }else{
                    stack[++stack_pointer] = left;
                    stack[++stack_pointer] = j-1;
                    left = i;
                }
            }
        }
    }	
}
