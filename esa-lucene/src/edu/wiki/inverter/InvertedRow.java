package edu.wiki.inverter;

import java.util.HashMap;
import java.util.Map;

public class InvertedRow {
	final String token;
	final Map<Long, Double> weigths = new HashMap<Long, Double>();
	
	public InvertedRow(String token) {
		this.token = token;
	}
	public void addWeigth(Long id, Double weigth) {
		weigths.put(id, weigth);
	}
}
