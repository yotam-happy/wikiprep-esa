package edu.wiki.util.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import edu.wiki.util.AbstractDBQueryOptimizer;

public class InlinkQueryOptimizer extends AbstractDBQueryOptimizer<Integer, Integer> {

	private static InlinkQueryOptimizer inlinkQueryOptimizer;
	
	public static InlinkQueryOptimizer getInstance() {
		if (inlinkQueryOptimizer == null) {
			inlinkQueryOptimizer = new InlinkQueryOptimizer();
		}
		return inlinkQueryOptimizer;
	}

	private InlinkQueryOptimizer() {
		super("SELECT i.target_id, i.inlink FROM inlinks i WHERE i.target_id IN (?)");
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
	protected String getLoadAllQuery() {
		return "SELECT i.target_id, i.inlink FROM inlinks i";
	}

}
