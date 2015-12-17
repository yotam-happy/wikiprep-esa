package edu.wiki.util;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class BulkDbInserter<T> {
	public static final int MAX_ROWS_PER_STATEMENT = 1000;
	PreparedStatement pstmt;
	int nColumns;
	int curRow;

	public BulkDbInserter(String tableName, String[] columns) {
		nColumns = columns.length;
		try {
			pstmt = buildStatement(tableName, columns, MAX_ROWS_PER_STATEMENT);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		curRow = 0;
	}
	
	protected abstract void setRowData(PreparedStatement pstmt, T data, int columnStartIndex) throws SQLException;
	
	public void addRow(T data) {
		try {
			setRowData(pstmt, data, curRow * nColumns + 1);
		} catch (SQLException e1) {
			throw new RuntimeException(e1);
		}
		curRow++;
		if (curRow == MAX_ROWS_PER_STATEMENT) {
			try {
				pstmt.execute();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
			curRow = 0;
		}
	}
	public void finalize() {
		if (curRow == 0){
			return;
		}
		for (int i = curRow; i < MAX_ROWS_PER_STATEMENT; i++){
			try {
				setRowData(pstmt, null, i * nColumns + 1);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
		try {
			pstmt.execute();
			pstmt.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	private PreparedStatement buildStatement(String tableName, String[] columns, int rows) throws SQLException {
		StringBuffer sb = new StringBuffer();
		sb.append("INSERT IGNORE ").append(tableName).append(" (");
		boolean first = true;
		for(String column : columns){
			sb.append(first ? column : "," + column);
			first = false;
		}
		sb.append(") values ");
		first = true;
		for(int i = 0; i < rows; i++) {
			sb.append(first ? "(" : ", (");
			boolean first2 = true;
			for(int j = 0; j < columns.length; j++) {
				sb.append(first2 ? "?" : ",?");
				first2 = false;
			}
			sb.append(")");
			first = false;
		}
		return WikiprepESAdb.getInstance().getConnection().prepareStatement(sb.toString());
	}
}
