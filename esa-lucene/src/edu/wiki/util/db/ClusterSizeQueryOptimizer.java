package edu.wiki.util.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import edu.wiki.util.AbstractDBQueryOptimizer;

public class ClusterSizeQueryOptimizer extends AbstractDBQueryOptimizer<Integer, Integer>{
	private static Map<String,ClusterSizeQueryOptimizer> instances = new HashMap<>();
	
	public static ClusterSizeQueryOptimizer getInstance(String baseTableNme) {
		ClusterSizeQueryOptimizer o = instances.get(baseTableNme);
		if (o == null) {
			o = new ClusterSizeQueryOptimizer(baseTableNme);
			instances.put(baseTableNme, o);
		}
		return o;
	}

	String baseTableNme;
	private ClusterSizeQueryOptimizer(String baseTableNme) {
		super("SELECT id, size FROM "+baseTableNme+"_lengths WHERE id IN (?)");
		setMaxCachEntries(100000);
		this.baseTableNme = baseTableNme;
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
		return "SELECT id, size FROM "+baseTableNme+"_lengths";
	}

}
