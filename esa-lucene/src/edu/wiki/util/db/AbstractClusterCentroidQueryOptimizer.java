package edu.wiki.util.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import edu.wiki.util.AbstractDBQueryOptimizer;

public class AbstractClusterCentroidQueryOptimizer extends AbstractDBQueryOptimizer<Integer, byte[]>{
	private String tableName;
	
	protected AbstractClusterCentroidQueryOptimizer(String tableName) {
		super("SELECT id, vector FROM " + tableName + " WHERE id IN (?)");
		this.tableName = tableName;
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
	protected byte[] getValueFromRs(ResultSet rs, byte[] oldValue) {
		try {
			return rs.getBytes(2);
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
		return "SELECT id, vector FROM " + tableName;
	}


}
