package edu.wiki.util.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import edu.wiki.util.AbstractDBQueryOptimizer;

public class WikiSubCategoryLinksQueryOptimizer extends AbstractDBQueryOptimizer<Integer, Set<Integer>> {
	private static WikiSubCategoryLinksQueryOptimizer query;
	
	public static WikiSubCategoryLinksQueryOptimizer getInstance() {
		if (query == null) {
			query = new WikiSubCategoryLinksQueryOptimizer();
		}
		return query;
	}

	private WikiSubCategoryLinksQueryOptimizer() {
		super("SELECT cl_from, cl_to FROM subcategorylinks WHERE cl_from IN (?)");
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
	protected Set<Integer> getValueFromRs(ResultSet rs, Set<Integer> oldValue) {
		try {
			Set<Integer> set = oldValue == null ? new HashSet<>(): oldValue;
			set.add(rs.getInt(2));
			return set;
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
		return "SELECT cl_from, cl_to FROM subcategorylinks";
	}

}
