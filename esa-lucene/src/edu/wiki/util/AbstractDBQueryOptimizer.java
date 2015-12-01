package edu.wiki.util;

import gnu.trove.THashMap;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

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
		cache = new THashMap<K,V>();
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
	protected abstract V getValueFromRs(ResultSet rs, V oldValue);

	/**
	 * Service method that must be implemented to get value from result set at 
	 * current position.
	 * not very pretty but seems to be a must because of the way java.sql works
	 */
	protected abstract K getKeyFromRs(ResultSet rs);

	/**
	 * Will be called by loadAll(). If this returns null then loadAll functionality
	 * is disabled. otherwise this should return the SQL needed to traverse the entire
	 * underlying data structure so it can be loaded into memory
	 * @return
	 */
	protected abstract String getLoadAllQuery();
	
	public Stream<Entry<K, V>> cacheStream() {
		return cache.entrySet().stream();
	}
	
	public V doQuery(K key) {
		if ((!getNoCacheMode()) && cache.containsKey(key)) {
			cache_hits += 1;
			return cache.get(key);
		} else {
			if (getAllLoadedMode()) {
				return null;
			}
			// if not in cache, and not allLoadedMode, do query
			cache_misses += 1;
			setKeyInPstmt(pstmt, 0, key);
			return executePstmt().get(key);
		}
	}
	
	public THashMap<K,V> doQuery(Set<K> keys) {
		final THashMap<K,V> result = new THashMap<K,V>();
		
		// Do the queries, up to maxSelectGrouping at a time
		int count = 0;
		for (K key : keys) {
			// if in cache add directly to results
			if ((!getNoCacheMode()) && cache.containsKey(key)) {
				cache_hits += 1;
				result.put(key, cache.get(key));
			} else {
				if (getAllLoadedMode()) {
					break;
				}
				// if not in cache, and not allLoadedMode, do query
				setKeyInPstmt(pstmt, count % maxSelectGrouping + 1, key);
				count++;
				if (count % maxSelectGrouping == 0) {
					result.putAll(executePstmt());
				}
				cache_misses += 1;
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
	
	private Map<K,V> executePstmt() {
		final Map<K,V> res = new THashMap<K,V>();
		try {
			res.clear();
			pstmt.execute();
	        ResultSet rs = pstmt.getResultSet();
	        while(rs.next()) {
	        	K k = getKeyFromRs(rs);
	        	V v = getValueFromRs(rs, res.get(k));
	        	res.put(k, v);
	        	addToCache(k, v);
	        }
			return res;
		}catch(SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	private void evacuateFromCache() {
		if (getAllLoadedMode() || getNoCacheMode()) {
			return;
		}
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
		if (getNoCacheMode()) {
			return;
		}
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

	/**
	 * This method sets cache to unlimited size and
	 * fills it with the entire table
	 * @throws SQLException 
	 */
	public void loadAll() throws SQLException {
		if (getLoadAllQuery() == null) {
			return;
		}
		setAllLoadedMode();
		cache.clear();
		PreparedStatement pstmtLoadAll = WikiprepESAdb.getInstance().getConnection()
				.prepareStatement(getLoadAllQuery());
		ResultSet rs = null;
		try {
	        pstmtLoadAll.execute();
	        rs = pstmtLoadAll.getResultSet();
	        while(rs.next()) {
	        	K k = getKeyFromRs(rs);
	        	V v = getValueFromRs(rs, cache.get(k));
	        	addToCache(k, v);
	        }
	        rs.close();
	        pstmtLoadAll.close();
		}catch(SQLException e) {
	        rs.close();
	        pstmtLoadAll.close();
			throw new RuntimeException(e);
		}
		
	}
	
	private void setAllLoadedMode() {
		maxCacheEntries = -1;
	}
	private boolean getAllLoadedMode() {
		return maxCacheEntries == -1;
	}
	private boolean getNoCacheMode() {
		return maxCacheEntries == 0;
	}
}
