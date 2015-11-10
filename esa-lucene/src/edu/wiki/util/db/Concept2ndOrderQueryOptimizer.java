package edu.wiki.util.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import edu.wiki.util.AbstractDBQueryOptimizer;

public class Concept2ndOrderQueryOptimizer extends AbstractDBQueryOptimizer<Integer, byte[]> {

	private static Concept2ndOrderQueryOptimizer term2ndOrderQueryOptimizer;
	
	public static Concept2ndOrderQueryOptimizer getInstance() {
		if (term2ndOrderQueryOptimizer == null) {
			term2ndOrderQueryOptimizer = new Concept2ndOrderQueryOptimizer();
		}
		return term2ndOrderQueryOptimizer;
	}

	private Concept2ndOrderQueryOptimizer() {
		super("SELECT id, vector FROM concept_2nd WHERE id IN (?)");
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
	protected String getLoadAllQuery() {
		return "SELECT id, vector FROM concept_2nd";
	}

}
