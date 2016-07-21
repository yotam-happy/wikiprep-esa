package edu.wiki.index;

import java.util.ArrayList;

import edu.wiki.util.TermVectorIterator;
import edu.wiki.util.db.TermQueryOptimizer;

public class SomeTests {
	static final double KEEP = 0.95;
	public static void main(String[] args) {
		
		
		
		TermQueryOptimizer.getInstance().loadAll();
		TermQueryOptimizer.getInstance().forEach((term,v)->{
			TermVectorIterator tvi = new TermVectorIterator(v);
			ArrayList<Double> arr = new ArrayList<>();
			while(tvi.next()){
				arr.add((double)tvi.getConceptScore());
			}
			arr.sort((x,y)->-Double.compare(x, y));
			int t = 0;
			double a = 0;
			for(int i = 0; i < arr.size(); i++){
				t++;
				a += arr.get(i);
				if (a > KEEP){
					break;
				}
			}
		});
	}

}
