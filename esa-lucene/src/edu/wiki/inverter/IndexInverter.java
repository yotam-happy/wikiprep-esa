package edu.wiki.inverter;

import java.util.Set;

public interface IndexInverter {
	public void recieveRow(long id, Set<IndexEntry> row);
	public void invert();
	public InvertedRow getInvertedRow();

}
