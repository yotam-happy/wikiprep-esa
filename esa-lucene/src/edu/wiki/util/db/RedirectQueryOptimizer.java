package edu.wiki.util.db;

import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import edu.wiki.util.AbstractDBQueryOptimizer;

public class RedirectQueryOptimizer extends AbstractDBQueryOptimizer<String, String>{ 
	private static RedirectQueryOptimizer instance;
	
	public static RedirectQueryOptimizer getInstance() {
		if (instance == null) {
			instance = new RedirectQueryOptimizer();
		}
		return instance;
	}

	private RedirectQueryOptimizer() {
		super("SELECT rd_from, rd_title FROM redirect WHERE rd_from IN (?)");
		setMaxCachEntries(100000);
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
	protected String getValueFromRs(ResultSet rs, String oldValue) {
		try {
			return new String(rs.getBytes(2), "UTF-8");
		} catch (SQLException | UnsupportedEncodingException e) {
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
	public String getLoadAllQuery() {
		return "SELECT rd_from, rd_title FROM redirect";
	}

}
