package edu.wiki.util.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import edu.wiki.util.AbstractDBQueryOptimizer;

public class ClusterSizeQueryOptimizer extends AbstractDBQueryOptimizer<Integer, Integer>{
	private static ClusterSizeQueryOptimizer instance;
	
	public static ClusterSizeQueryOptimizer getInstance() {
		if (instance == null) {
			instance = new ClusterSizeQueryOptimizer();
		}
		return instance;
	}

	private ClusterSizeQueryOptimizer() {
		super("SELECT id, size FROM cluster_lengths WHERE id IN (?)");
		setMaxCachEntries(100000);
	}

	@Override
	protected void setKeyInPstmt(PreparedStatement pstmt, int pos, Integer key) {
		try {
			if (key == null) {
				pstmt.setNull(pos, java.sql.Types.INTEGER);
			} else {
				pstmt.setInt(pos, key);
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected Integer getValueFromRs(ResultSet rs, Integer oldValue) {
		try {
			return rs.getInt(2);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected Integer getKeyFromRs(ResultSet rs) {
		try {
			return rs.getInt(1);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String getLoadAllQuery() {
		return "SELECT t.term, t.idf FROM terms t";
	}

}
