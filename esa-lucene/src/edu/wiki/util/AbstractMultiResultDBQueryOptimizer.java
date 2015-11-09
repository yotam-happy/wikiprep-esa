package edu.wiki.util;

import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractMultiResultDBQueryOptimizer<K extends Comparable<K>,V> extends
		AbstractDBQueryOptimizer<K,Set<V>> {
	public AbstractMultiResultDBQueryOptimizer(String query) {
		super(query);
	}

	abstract protected V getSingleValueFromRs(ResultSet rs);

	@Override
	protected Set<V> getValueFromRs(ResultSet rs, Set<V> oldValue) {
		if (oldValue == null) {
			oldValue = new HashSet<V>();
		}
		oldValue.add(getSingleValueFromRs(rs));
		return oldValue;
	}
}
