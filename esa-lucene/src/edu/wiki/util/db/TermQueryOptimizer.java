package edu.wiki.util.db;

import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import edu.wiki.util.AbstractDBQueryOptimizer;

public class TermQueryOptimizer extends AbstractDBQueryOptimizer<String, byte[]> {

	private static TermQueryOptimizer termQueryOptimizer;
	
	public static TermQueryOptimizer getInstance() {
		if (termQueryOptimizer == null) {
			termQueryOptimizer = new TermQueryOptimizer();
		}
		return termQueryOptimizer;
	}

	private TermQueryOptimizer() {
		super("SELECT t.term, t.vector FROM idx t WHERE t.term IN (?)");
	}

	@Override
	protected void setKeyInPstmt(PreparedStatement pstmt, int pos, String key) {
        try {
			pstmt.setBytes(pos, key == null ? null : key.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException | SQLException e) {
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
	protected String getKeyFromRs(ResultSet rs) {
		try {
			return new String(rs.getBytes(1), "UTF-8");
		} catch (SQLException | UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected String getLoadAllQuery() {
		return "SELECT t.term, t.vector FROM idx t";
	}
}
