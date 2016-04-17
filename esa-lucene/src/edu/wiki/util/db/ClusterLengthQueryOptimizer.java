package edu.wiki.util.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import edu.wiki.util.AbstractDBQueryOptimizer;

public class ClusterLengthQueryOptimizer extends AbstractDBQueryOptimizer<Integer, Double>{
	private static Map<String,ClusterLengthQueryOptimizer> instances = new HashMap<>();
	
	public static ClusterLengthQueryOptimizer getInstance(String baseTableNme) {
		ClusterLengthQueryOptimizer o = instances.get(baseTableNme);
		if (o == null) {
			o = new ClusterLengthQueryOptimizer(baseTableNme);
			instances.put(baseTableNme, o);
		}
		return o;
	}

	String baseTableNme;
	private ClusterLengthQueryOptimizer(String baseTableNme) {
		super("SELECT id, len FROM " + baseTableNme + "_lengths WHERE id IN (?)");
		this.baseTableNme = baseTableNme;
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
	protected Double getValueFromRs(ResultSet rs, Double oldValue) {
		try {
			return rs.getDouble(2);
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
		return "SELECT id, len FROM " + baseTableNme + "_lengths";
	}

}
