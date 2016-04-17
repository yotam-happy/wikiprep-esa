package edu.wiki.util.db;

import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import edu.wiki.util.AbstractDBQueryOptimizer;

public class WikiCategoryNamesQueryOptimizer extends AbstractDBQueryOptimizer<Integer, String>{
	private static WikiCategoryNamesQueryOptimizer query;
	
	public static WikiCategoryNamesQueryOptimizer getInstance() {
		if (query == null) {
			query = new WikiCategoryNamesQueryOptimizer();
		}
		return query;
	}

	private WikiCategoryNamesQueryOptimizer() {
		super("SELECT cat_id, cat_title FROM category WHERE cat_id IN (?)");
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
	protected String getValueFromRs(ResultSet rs, String oldValue) {
		try {
			return new String(rs.getBytes(2), "UTF-8");
		} catch (SQLException | UnsupportedEncodingException e) {
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
		return "SELECT cat_id, cat_title FROM category";
	}
}
