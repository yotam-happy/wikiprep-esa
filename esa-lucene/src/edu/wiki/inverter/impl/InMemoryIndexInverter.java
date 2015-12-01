package edu.wiki.inverter.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import edu.wiki.inverter.IndexEntry;
import edu.wiki.inverter.IndexInverter;
import edu.wiki.inverter.InvertedRow;

public class InMemoryIndexInverter implements IndexInverter{

	Set<InternalIndexEntry> index = new HashSet<>();
	List<InternalIndexEntry> inverted;
	int i;
	
	public InMemoryIndexInverter() {
	}

	@Override
	public void recieveRow(long id, Set<IndexEntry> row) {
		row.forEach((e)->index.add(new InternalIndexEntry(id, e)));
	}
	
	@Override
	public void invert() {
		inverted = index.stream().sorted((e1,e2)->e1.token.compareTo(e2.token)).collect(Collectors.toList());
		i = 0;
	}
	
	@Override
	public InvertedRow getInvertedRow() {
		if (i >= inverted.size()) {
			return null;
		}
		String token = inverted.get(i).token;
		InvertedRow row = new InvertedRow(token);
		while (inverted.get(i).token.equals(token)) {
			row.addWeigth(inverted.get(i).id, inverted.get(i).weigth);
		}
		return row;
	}
	
	class InternalIndexEntry {
		long id;
		String token;
		double weigth;
		InternalIndexEntry(long id, IndexEntry e) {
			this.id = id;
			this.token = e.getToken();
			this.weigth = e.getWeigth();
		}
	}
}
