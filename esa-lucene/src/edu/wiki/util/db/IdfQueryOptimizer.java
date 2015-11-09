package edu.wiki.util.db;

import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import edu.wiki.util.AbstractDBQueryOptimizer;

public class IdfQueryOptimizer extends AbstractDBQueryOptimizer<String, Float> {

	private static IdfQueryOptimizer idfQueryOptimizer;
	
	public static IdfQueryOptimizer getInstance() {
		if (idfQueryOptimizer == null) {
			idfQueryOptimizer = new IdfQueryOptimizer();
		}
		return idfQueryOptimizer;
	}

	private IdfQueryOptimizer() {
		super("SELECT t.term, t.idf FROM terms t WHERE t.term IN (?)");
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
	protected Float getValueFromRs(ResultSet rs, Float oldValue) {
		try {
			return rs.getFloat(2);
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

}
