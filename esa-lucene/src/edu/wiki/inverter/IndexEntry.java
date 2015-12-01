package edu.wiki.inverter;

public class IndexEntry {
	final String token;
	final double weigth;
	
	public IndexEntry(String token, double weigth) {
		this.token = token;
		this.weigth = weigth;
	}
	
	public String getToken() {
		return token;
	}
	public double getWeigth() {
		return weigth;
	}
}
