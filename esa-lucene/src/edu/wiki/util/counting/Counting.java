package edu.wiki.util.counting;

public class Counting {
	private int c = 0;
	private int reportGap;
	private String text = null;
	public Counting(int reportGap){
		this.reportGap = reportGap;
	}
	public Counting(int reportGap, String text){
		this.reportGap = reportGap;
		this.text = text;
	}
	public synchronized void addOne(){
		c++;
		if (c % reportGap == 0){
			System.out.println((text == null ? "Processed: " : text + " Processed: ") + c);
		}
	}
}
