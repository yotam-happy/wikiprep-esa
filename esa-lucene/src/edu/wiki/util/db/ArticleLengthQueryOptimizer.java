package edu.wiki.util.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import edu.wiki.util.AbstractDBQueryOptimizer;

public class ArticleLengthQueryOptimizer extends AbstractDBQueryOptimizer<Integer, Double>{
	private static ArticleLengthQueryOptimizer instance;
	
	public static ArticleLengthQueryOptimizer getInstance() {
		if (instance == null) {
			instance = new ArticleLengthQueryOptimizer();
		}
		return instance;
	}

	private ArticleLengthQueryOptimizer() {
		super("SELECT id, len FROM article_lengths WHERE id IN (?)");
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
		return "SELECT t.term, t.idf FROM terms t";
	}

}
