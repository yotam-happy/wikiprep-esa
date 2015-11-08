package edu.wiki.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to optimize simple "by id" queries against static
 * tables (that never change) in two ways:
 * 1. agregating multiple 'WHERE col=X' into 'WHERE col IN (l)'
 * 2. managing a basic cache of queries
 * 
 */
public abstract class AbstractDBQueryOptimizer<K extends Comparable<K>, V> {
	private Map<K, V> cache;
	private final String query;
	private PreparedStatement pstmt;

	private int maxCacheEntries = 1000;
	private int maxSelectGrouping = 100;

	private long cache_hits = 0;
	private long cache_misses = 0;
	/**
	 * 
	 * @param max_cache_entries
	 * @param query				the query to perform. Expected in a format similar to:
	 * 							"SELECT [key and value columns] FROM [table] WHERE [id] IN (?)"
	 * 							The important part is IN(?). This will be filled by
	 * 							list of ids
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws FileNotFoundException 
	 * @throws ClassNotFoundException 
	 */
	public AbstractDBQueryOptimizer(String query) {
		cache = new HashMap<K,V>();
		this.query = query;
		// initializes pstmt
		setMaxSelectGrouping(maxSelectGrouping);
	}

	/**
	 * Service method that must be implemented to set value into pstmt at position pos
	 * not very pretty but seems to be a must because of the way java.sql works
	 * Note: this method must know how to handle null values
	 */
	protected abstract void setKeyInPstmt(PreparedStatement pstmt, int pos, K key);

	/**
	 * Service method that must be implemented to get value from result set at 
	 * current position.
	 * not very pretty but seems to be a must because of the way java.sql works
	 */
	protected abstract V getValueFromRs(ResultSet rs);

	/**
	 * Service method that must be implemented to get value from result set at 
	 * current position.
	 * not very pretty but seems to be a must because of the way java.sql works
	 */
	protected abstract K getKeyFromRs(ResultSet rs);
	
	public Map<K,V> doQuery(Set<K> keys) throws SQLException {
		final List<K> toQuery = new ArrayList<K>();
		final Map<K,V> result = new HashMap<K,V>();
		
		// for keys that are found in cache, get the value
		// for the others, keep for query
		for (K key : keys) {
			if (cache.containsKey(key)) {
				cache_hits += 1;
				result.put(key, cache.get(key));
			} else {
				toQuery.add(key);
				cache_misses += 1;
			}
		}
		
		System.out.println("cache hits: " + cache_hits + " cache misses: " + cache_misses);
		
		// Do the queries, up to maxSelectGrouping at a time
		int count = 0;
		for (K key : toQuery) {
			setKeyInPstmt(pstmt, count % maxSelectGrouping + 1, key);
			count++;
			if (count % maxSelectGrouping == 0) {
				result.putAll(executePstmt());
			}
		}
		// if there are some elements left, do them
		if (count % maxSelectGrouping != 0) {
			// fill left overs with null
			for (int i = count % maxSelectGrouping; i < maxSelectGrouping; i++) {
				setKeyInPstmt(pstmt, i + 1, null);
			}
			result.putAll(executePstmt());
		}

		return result;
	}
	
	private Map<K,V> executePstmt() throws SQLException {
        Map<K,V> res = new HashMap<K,V>();
		pstmt.execute();
        ResultSet rs = pstmt.getResultSet();
        while(rs.next()) {
        	V v = getValueFromRs(rs);
        	K k = getKeyFromRs(rs);
        	res.put(k, v);
        	addToCache(k, v);
        }
		return res;
	}
	
	private void evacuateFromCache() {
		// If cache is full then remove one element.
		// maybe someday i need to change this ugly little random 
		// evacuation policy to something more meaningful...
		while (cache.size() >= maxCacheEntries) {
			int toRemove = (int)(Math.random() * cache.size());
			K keyToRemove = null;
			for(K tmp : cache.keySet()) {
				keyToRemove = tmp;
				toRemove--;
				if (toRemove == 0) {
					break;
				}
			}
			cache.remove(keyToRemove);
		}
	}
	private void addToCache(K k, V v) {
		evacuateFromCache();
		cache.put(k, v);
	}

	public void setMaxSelectGrouping(int maxSelectGrouping) {
		this.maxSelectGrouping = maxSelectGrouping;
		try {
			this.pstmt = WikiprepESAdb.getInstance().getConnection()
					.prepareStatement(expandQuery(query, maxSelectGrouping));
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void setMaxCachEntries(int maxCacheEntries) {
		this.maxCacheEntries = maxCacheEntries;
		evacuateFromCache();
	}
	
	private static String expandQuery(String q, int k) {
		if (q.indexOf('?') < 0) {
			return q;
		}
		StringBuffer sb = new StringBuffer();
		sb.append(q.substring(0, q.indexOf('?') + 1));
		for (int i = 0; i < k - 1; i++) {
			sb.append(",?");
		}
		sb.append(q.substring(q.indexOf('?') + 1));
		return sb.toString();
	}
}