package edu.wiki.util.db;

import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import edu.wiki.util.AbstractDBQueryOptimizer;

public class ArticleTitleQueryOptimizer extends AbstractDBQueryOptimizer<Integer, String>{ 
	private static ArticleTitleQueryOptimizer instance;
	
	public static ArticleTitleQueryOptimizer getInstance() {
		if (instance == null) {
			instance = new ArticleTitleQueryOptimizer();
		}
		return instance;
	}

	private ArticleTitleQueryOptimizer() {
		super("SELECT id, title FROM article WHERE id IN (?)");
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
		return "SELECT id, title FROM article";
	}

}
