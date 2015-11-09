package edu.wiki.util.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import edu.wiki.util.AbstractMultiResultDBQueryOptimizer;

public class LinkTargetsQueryOptimizer extends AbstractMultiResultDBQueryOptimizer<Integer, Integer>{

	private static LinkTargetsQueryOptimizer linkTargetsQueryOptimizer;
	
	public static LinkTargetsQueryOptimizer getInstance() {
		if (linkTargetsQueryOptimizer == null) {
			linkTargetsQueryOptimizer = new LinkTargetsQueryOptimizer();
		}
		return linkTargetsQueryOptimizer;
	}

	public LinkTargetsQueryOptimizer() {
		super("SELECT source_id, target_id FROM pagelinks WHERE source_id IN (?)");
		setMaxSelectGrouping(1);
	}

	@Override
	protected Integer getSingleValueFromRs(ResultSet rs) {
		try {
			return rs.getInt(2);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
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
	protected Integer getKeyFromRs(ResultSet rs) {
		try {
			return rs.getInt(1);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

}
