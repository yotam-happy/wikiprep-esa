package edu.wiki.util.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import edu.wiki.util.AbstractDBQueryOptimizer;

public abstract class AbstractClusterMembershipQueryOptimizer extends AbstractDBQueryOptimizer<Integer, AbstractClusterMembershipQueryOptimizer.MembershipData>{

	private String tableName;
	
	protected AbstractClusterMembershipQueryOptimizer(String tableName) {
		super("SELECT concept, cluster, distance FROM " + tableName + " WHERE concept IN (?)");
		this.tableName = tableName;
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
	protected MembershipData getValueFromRs(ResultSet rs, MembershipData oldValue) {
		try {
			return new MembershipData(rs.getInt(2),rs.getFloat(3));
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
		return "SELECT concept, cluster, distance FROM " + tableName;
	}
	
	public class MembershipData {
		public final int cluster;
		public final double distance;
		public MembershipData (int cluster, double distance){
			this.cluster = cluster;
			this.distance = distance;
		}
	}
}
