package edu.wiki.util.counting;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class Counting {
	private int c = 0;
	private int interval;
	private int lastReport = 0;
	private String text = null;
	Instant start;
	public Counting(int interval){
		this.interval = interval;
		start = Instant.now();
	}
	public Counting(int interval, String text){
		this.interval = interval;
		this.text = text;
		start = Instant.now();
	}
	public synchronized void addOne(){
		c++;
		
    	Duration dur = Duration.between(start, Instant.now());
		if (dur.get(ChronoUnit.SECONDS) >= lastReport + interval){
			lastReport+=interval;
	    	double rate = (double)c / ((double)dur.get(ChronoUnit.SECONDS) / 60.0);
			System.out.println((text == null ? "Processed: " : text + " Processed: ") + c + ", avg: " + rate);
		}
	}
	public int count(){
		return c;
	}
}
