package edu.wiki.util.db;

import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import edu.wiki.util.AbstractDBQueryOptimizer;

public class SurfaceNamesQueryOptimizer extends AbstractDBQueryOptimizer<String, Set<Integer>>{
	private static SurfaceNamesQueryOptimizer articleQueryOptimizer;
	
	public static SurfaceNamesQueryOptimizer getInstance() {
		if (articleQueryOptimizer == null) {
			articleQueryOptimizer = new SurfaceNamesQueryOptimizer();
		}
		return articleQueryOptimizer;
	}

	private SurfaceNamesQueryOptimizer() {
		super("SELECT name, concept_id FROM surface_names WHERE name IN (?)");
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
	protected String getKeyFromRs(ResultSet rs) {
		try {
			return new String(rs.getBytes(1), "UTF-8");
		} catch (SQLException | UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected String getLoadAllQuery() {
		return "SELECT name, concept_id FROM surface_names";
	}
}
